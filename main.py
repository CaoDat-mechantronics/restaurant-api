import asyncio
import hashlib
import json
import os
import re
import ssl
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

import firebase_admin
import jwt
import paho.mqtt.client as mqtt
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, WebSocket, WebSocketDisconnect, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from firebase_admin import credentials, db
from jwt.exceptions import InvalidTokenError
from pwdlib import PasswordHash
from pydantic import BaseModel, Field


# =========================================================
# LOAD ENV
# =========================================================

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")


# =========================================================
# CẤU HÌNH CHUNG
# =========================================================

TOTAL_TABLES = int(os.getenv("TOTAL_TABLES", "10"))

# Firebase Realtime Database
FIREBASE_DATABASE_URL = os.getenv("FIREBASE_DATABASE_URL", "").strip()
FIREBASE_ROOT_PATH = os.getenv("FIREBASE_ROOT_PATH", "restaurant").strip().strip("/")
FIREBASE_SERVICE_ACCOUNT_JSON = os.getenv("FIREBASE_SERVICE_ACCOUNT_JSON", "").strip()
FIREBASE_SERVICE_ACCOUNT_PATH = os.getenv("FIREBASE_SERVICE_ACCOUNT_PATH", "").strip()

# MQTT / HiveMQ
MQTT_HOST = os.getenv("MQTT_HOST", "").strip()
MQTT_PORT = int(os.getenv("MQTT_PORT", "8883"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME", "").strip()
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "").strip()

MQTT_CONFIGURED = bool(
    MQTT_HOST
    and MQTT_USERNAME
    and MQTT_PASSWORD
)

# CORS. Ví dụ:
# CORS_ORIGINS=https://your-frontend.pages.dev,https://example.com
# Để * trong giai đoạn phát triển.
CORS_ORIGINS_RAW = os.getenv("CORS_ORIGINS", "*").strip()
if CORS_ORIGINS_RAW == "*":
    CORS_ORIGINS = ["*"]
else:
    CORS_ORIGINS = [
        origin.strip()
        for origin in CORS_ORIGINS_RAW.split(",")
        if origin.strip()
    ]

# JWT / account management
JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "").strip()
JWT_ALGORITHM = "HS256"
JWT_ACCESS_TOKEN_EXPIRE_MINUTES = int(
    os.getenv("JWT_ACCESS_TOKEN_EXPIRE_MINUTES", "120")
)
INITIAL_ADMIN_USERNAME = os.getenv("INITIAL_ADMIN_USERNAME", "").strip()
INITIAL_ADMIN_PASSWORD = os.getenv("INITIAL_ADMIN_PASSWORD", "")


# =========================================================
# PYDANTIC MODELS
# =========================================================

class FoodItem(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    price: int = Field(gt=0)
    quantity: int = Field(gt=0)
    note: str = Field(default="", max_length=500)


class OrderCreate(BaseModel):
    tableNumber: int = Field(gt=0)
    foods: list[FoodItem]


class DeliveryUpdate(BaseModel):
    delivered: bool


class QuantityUpdate(BaseModel):
    quantity: int = Field(gt=0)


class RobotDispatchUpdate(BaseModel):
    robot: int = Field(ge=1, le=2)


class OrderItemEdit(BaseModel):
    id: int = Field(gt=0)
    quantity: int = Field(ge=0)
    note: str = Field(default="", max_length=500)


class TableItemsUpdate(BaseModel):
    items: list[OrderItemEdit]


class LoginRequest(BaseModel):
    username: str = Field(min_length=3, max_length=50)
    password: str = Field(min_length=8, max_length=200)


class PasswordChangeRequest(BaseModel):
    current_password: str = Field(min_length=8, max_length=200)
    new_password: str = Field(min_length=8, max_length=200)


class ManagerUserCreate(BaseModel):
    username: str = Field(min_length=3, max_length=50)
    password: str = Field(min_length=8, max_length=200)
    active: bool = True


class ManagerUserUpdate(BaseModel):
    username: str | None = Field(default=None, min_length=3, max_length=50)
    new_password: str | None = Field(default=None, min_length=8, max_length=200)
    active: bool | None = None


# =========================================================
# FIREBASE INITIALIZATION
# =========================================================

def init_firebase():
    """
    Hỗ trợ 3 cách credentials, theo thứ tự ưu tiên:

    1) FIREBASE_SERVICE_ACCOUNT_JSON
       - Dùng tốt trên Render.
       - Giá trị là toàn bộ JSON service account ở dạng một dòng.

    2) FIREBASE_SERVICE_ACCOUNT_PATH
       - Dùng tốt ở local.
       - Ví dụ: serviceAccountKey.json

    3) GOOGLE_APPLICATION_CREDENTIALS / Application Default Credentials
       - Dùng khi môi trường đã cấu hình ADC.
    """
    try:
        return firebase_admin.get_app()
    except ValueError:
        pass

    if not FIREBASE_DATABASE_URL:
        raise RuntimeError(
            "Thiếu FIREBASE_DATABASE_URL. Hãy cấu hình trong .env hoặc Render Environment."
        )

    credential = None

    if FIREBASE_SERVICE_ACCOUNT_JSON:
        try:
            service_account_info = json.loads(FIREBASE_SERVICE_ACCOUNT_JSON)
        except json.JSONDecodeError as error:
            raise RuntimeError(
                "FIREBASE_SERVICE_ACCOUNT_JSON không phải JSON hợp lệ."
            ) from error

        credential = credentials.Certificate(service_account_info)

    elif FIREBASE_SERVICE_ACCOUNT_PATH:
        service_account_path = Path(FIREBASE_SERVICE_ACCOUNT_PATH)

        if not service_account_path.is_absolute():
            service_account_path = BASE_DIR / service_account_path

        if not service_account_path.exists():
            raise RuntimeError(
                f"Không tìm thấy Firebase service account: {service_account_path}"
            )

        credential = credentials.Certificate(str(service_account_path))

    else:
        # credentials.ApplicationDefault() sẽ sử dụng ADC, bao gồm
        # GOOGLE_APPLICATION_CREDENTIALS nếu biến này được cấu hình.
        credential = credentials.ApplicationDefault()

    app_instance = firebase_admin.initialize_app(
        credential,
        {
            "databaseURL": FIREBASE_DATABASE_URL,
        },
    )

    print(f"[FIREBASE] Connected: {FIREBASE_DATABASE_URL}")
    print(f"[FIREBASE] Root path: /{FIREBASE_ROOT_PATH}")

    return app_instance

def normalize_firebase_collection(data) -> dict:
    """
    Firebase Realtime Database có thể trả collection:
    - dict nếu key không liên tục
    - list nếu key là số liên tục: 1, 2, 3, ...

    Hàm này chuẩn hóa cả hai thành:
    {
        "1": {...},
        "2": {...}
    }
    """

    if data is None:
        return {}

    # =========================================
    # FIREBASE TRẢ DICT
    # =========================================

    if isinstance(data, dict):
        return {
            str(key): value
            for key, value in data.items()
            if isinstance(value, dict)
        }

    # =========================================
    # FIREBASE TRẢ LIST
    # =========================================

    if isinstance(data, list):

        result = {}

        for index, value in enumerate(data):

            if not isinstance(value, dict):
                continue

            result[str(index)] = value

        return result

    return {}


def firebase_path(child_path: str = "") -> str:
    child_path = str(child_path or "").strip().strip("/")

    if FIREBASE_ROOT_PATH and child_path:
        return f"/{FIREBASE_ROOT_PATH}/{child_path}"

    if FIREBASE_ROOT_PATH:
        return f"/{FIREBASE_ROOT_PATH}"

    if child_path:
        return f"/{child_path}"

    return "/"


def root_ref():
    return db.reference(firebase_path())


def order_items_ref():
    return db.reference(firebase_path("order_items"))


def meta_ref():
    return db.reference(firebase_path("meta"))


def item_ref(item_id: int):
    return db.reference(firebase_path(f"order_items/{int(item_id)}"))


def users_ref():
    return db.reference(firebase_path("users"))


def username_indexes_ref():
    return db.reference(firebase_path("username_indexes"))


# =========================================================
# AUTH / JWT / ROLE HELPERS
# =========================================================

password_hasher = PasswordHash.recommended()
bearer_scheme = HTTPBearer(auto_error=False)
USERNAME_PATTERN = re.compile(r"^[A-Za-z0-9_.-]{3,50}$")


def validate_auth_configuration():
    if len(JWT_SECRET_KEY) < 32:
        raise RuntimeError(
            "JWT_SECRET_KEY phải có ít nhất 32 ký tự. "
            "Hãy đặt secret ngẫu nhiên trong .env / Render Environment."
        )

    if JWT_ACCESS_TOKEN_EXPIRE_MINUTES <= 0:
        raise RuntimeError("JWT_ACCESS_TOKEN_EXPIRE_MINUTES phải > 0.")


def normalize_username(username: str) -> str:
    value = str(username or "").strip().lower()

    if not USERNAME_PATTERN.fullmatch(value):
        raise HTTPException(
            status_code=400,
            detail=(
                "Username phải dài 3-50 ký tự và chỉ gồm chữ, số, "
                "_ - hoặc dấu chấm."
            ),
        )

    return value


def username_index_key(username: str) -> str:
    normalized = normalize_username(username)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def public_user(user: dict) -> dict:
    return {
        "id": str(user.get("id") or ""),
        "username": str(user.get("username") or ""),
        "role": str(user.get("role") or "manager"),
        "active": bool(user.get("active", True)),
        "created_at": str(user.get("created_at") or ""),
        "updated_at": str(user.get("updated_at") or ""),
    }


def get_user_by_id(user_id: str):
    data = users_ref().child(str(user_id)).get()
    if not isinstance(data, dict):
        return None
    return data


def get_user_by_username(username: str):
    normalized = normalize_username(username)
    index_key = username_index_key(normalized)
    user_id = username_indexes_ref().child(index_key).get()

    if not user_id:
        return None

    user = get_user_by_id(str(user_id))
    if not user:
        return None

    if str(user.get("username_normalized") or "") != normalized:
        return None

    return user


def create_user_record(username: str, password: str, role: str, active: bool = True):
    normalized = normalize_username(username)

    if role not in ("manager", "admin"):
        raise HTTPException(status_code=400, detail="Role không hợp lệ.")

    if len(password) < 8:
        raise HTTPException(status_code=400, detail="Mật khẩu phải có ít nhất 8 ký tự.")

    if get_user_by_username(normalized) is not None:
        raise HTTPException(status_code=409, detail="Username đã tồn tại.")

    user_id = uuid.uuid4().hex
    index_key = username_index_key(normalized)
    now_iso = datetime.now(timezone.utc).isoformat()

    record = {
        "id": user_id,
        "username": normalized,
        "username_normalized": normalized,
        "password_hash": password_hasher.hash(password),
        "role": role,
        "active": bool(active),
        "token_version": 0,
        "created_at": now_iso,
        "updated_at": now_iso,
    }

    # Reserve username index first to reduce duplicate-user races.
    index_ref = username_indexes_ref().child(index_key)

    def reserve_index(current):
        if current is not None:
            raise ValueError("USERNAME_EXISTS")
        return user_id

    try:
        index_ref.transaction(reserve_index)
    except Exception as error:
        if "USERNAME_EXISTS" in str(error):
            raise HTTPException(status_code=409, detail="Username đã tồn tại.") from error
        raise

    try:
        users_ref().child(user_id).set(record)
    except Exception:
        index_ref.delete()
        raise

    return record


def ensure_initial_admin():
    data = users_ref().get() or {}
    existing_admin = False

    if isinstance(data, dict):
        existing_admin = any(
            isinstance(user, dict)
            and str(user.get("role") or "") == "admin"
            for user in data.values()
        )

    if existing_admin:
        return

    if not INITIAL_ADMIN_USERNAME or not INITIAL_ADMIN_PASSWORD:
        raise RuntimeError(
            "Chưa có tài khoản admin. Hãy cấu hình INITIAL_ADMIN_USERNAME và "
            "INITIAL_ADMIN_PASSWORD cho lần khởi tạo đầu tiên."
        )

    admin = create_user_record(
        INITIAL_ADMIN_USERNAME,
        INITIAL_ADMIN_PASSWORD,
        role="admin",
        active=True,
    )
    print(f"[AUTH] Initial admin created: {admin['username']}")


def authenticate_user(username: str, password: str):
    user = get_user_by_username(username)

    if not user or not bool(user.get("active", True)):
        return None

    password_hash = str(user.get("password_hash") or "")

    try:
        valid = bool(password_hash) and password_hasher.verify(password, password_hash)
    except Exception:
        valid = False

    return user if valid else None


def create_access_token(user: dict):
    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(minutes=JWT_ACCESS_TOKEN_EXPIRE_MINUTES)

    payload = {
        "sub": str(user["id"]),
        "username": str(user["username"]),
        "role": str(user["role"]),
        "ver": int(user.get("token_version") or 0),
        "iat": now,
        "exp": expires_at,
    }

    token = jwt.encode(payload, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)
    return token, expires_at


def verify_access_token(token: str):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Phiên đăng nhập không hợp lệ hoặc đã hết hạn.",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        payload = jwt.decode(
            token,
            JWT_SECRET_KEY,
            algorithms=[JWT_ALGORITHM],
        )
        user_id = str(payload.get("sub") or "")
        token_version = int(payload.get("ver") or 0)
    except (InvalidTokenError, TypeError, ValueError):
        raise credentials_exception

    if not user_id:
        raise credentials_exception

    user = get_user_by_id(user_id)

    if not user or not bool(user.get("active", True)):
        raise credentials_exception

    if int(user.get("token_version") or 0) != token_version:
        raise credentials_exception

    if str(user.get("role") or "") not in ("manager", "admin"):
        raise credentials_exception

    return user


def get_current_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
):
    if credentials is None or credentials.scheme.lower() != "bearer":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Bạn chưa đăng nhập.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return verify_access_token(credentials.credentials)


def require_admin(current_user: dict = Depends(get_current_user)):
    if str(current_user.get("role") or "") != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Chỉ tài khoản admin được phép thực hiện thao tác này.",
        )
    return current_user


def bump_token_version(user_id: str):
    ref = users_ref().child(str(user_id)).child("token_version")

    def increment(current):
        try:
            value = int(current or 0)
        except (TypeError, ValueError):
            value = 0
        return value + 1

    return int(ref.transaction(increment))


def change_username(user: dict, new_username: str):
    old_normalized = str(user.get("username_normalized") or user.get("username") or "")
    new_normalized = normalize_username(new_username)

    if old_normalized == new_normalized:
        return user

    if get_user_by_username(new_normalized) is not None:
        raise HTTPException(status_code=409, detail="Username đã tồn tại.")

    user_id = str(user["id"])
    old_key = username_index_key(old_normalized)
    new_key = username_index_key(new_normalized)
    new_index_ref = username_indexes_ref().child(new_key)

    def reserve_index(current):
        if current is not None:
            raise ValueError("USERNAME_EXISTS")
        return user_id

    try:
        new_index_ref.transaction(reserve_index)
    except Exception as error:
        if "USERNAME_EXISTS" in str(error):
            raise HTTPException(status_code=409, detail="Username đã tồn tại.") from error
        raise

    try:
        now_iso = datetime.now(timezone.utc).isoformat()
        root_ref().update(
            {
                f"users/{user_id}/username": new_normalized,
                f"users/{user_id}/username_normalized": new_normalized,
                f"users/{user_id}/updated_at": now_iso,
                f"username_indexes/{old_key}": None,
            }
        )
    except Exception:
        new_index_ref.delete()
        raise

    return get_user_by_id(user_id)


# =========================================================
# FIREBASE DATA HELPERS
# =========================================================

def firebase_item_to_dict(item_id, data):
    data = data or {}

    quantity = int(data.get("quantity") or 0)
    unit_price = int(data.get("unit_price") or 0)

    assigned_robot = data.get("assigned_robot")
    if assigned_robot is not None:
        try:
            assigned_robot = int(assigned_robot)
        except (TypeError, ValueError):
            assigned_robot = None

    return {
        "id": int(data.get("id") or item_id),
        "order_code": str(data.get("order_code") or ""),
        "table_number": int(data.get("table_number") or 0),
        "food_name": str(data.get("food_name") or ""),
        "quantity": quantity,
        "unit_price": unit_price,
        "delivered": bool(data.get("delivered", False)),
        "note": str(data.get("note") or ""),
        "cooking_status": int(data.get("cooking_status") or 0),
        "assigned_robot": assigned_robot,
        "robot_dispatched": bool(data.get("robot_dispatched", False)),
        "delivery_status": str(data.get("delivery_status") or "waiting"),
        "dispatch_command_id": data.get("dispatch_command_id"),
        "created_at": str(data.get("created_at") or ""),
        "item_total": quantity * unit_price,
    }




def get_all_items_raw() -> dict:

    data = order_items_ref().get()

    normalized = normalize_firebase_collection(
        data
    )

    print(
        f"[FIREBASE] get_all_items_raw: "
        f"type={type(data).__name__}, "
        f"items={len(normalized)}"
    )

    return normalized


def get_all_items() -> list[dict]:
    data = get_all_items_raw()

    items = [
        firebase_item_to_dict(item_id, item_data)
        for item_id, item_data in data.items()
        if isinstance(item_data, dict)
    ]

    items.sort(key=lambda item: item["id"], reverse=True)
    return items


def get_item(item_id: int):
    data = item_ref(item_id).get()

    if not isinstance(data, dict):
        return None

    return firebase_item_to_dict(item_id, data)


def get_items_by_table(
    table_number: int
) -> list[dict]:

    snapshot = (
        order_items_ref()
        .order_by_child(
            "table_number"
        )
        .equal_to(
            int(table_number)
        )
        .get()
    )

    snapshot = (
        normalize_firebase_collection(
            snapshot
        )
    )

    items = [
        firebase_item_to_dict(
            item_id,
            item_data
        )
        for item_id, item_data
        in snapshot.items()
    ]

    items.sort(
        key=lambda item:
            item["id"]
    )

    return items

def get_items_by_order_code(
    order_code: str
) -> list[dict]:

    snapshot = (
        order_items_ref()
        .order_by_child(
            "order_code"
        )
        .equal_to(
            str(order_code)
        )
        .get()
    )

    snapshot = (
        normalize_firebase_collection(
            snapshot
        )
    )

    items = [
        firebase_item_to_dict(
            item_id,
            item_data
        )
        for item_id, item_data
        in snapshot.items()
    ]

    items.sort(
        key=lambda item:
            item["id"]
    )

    return items


def get_table_summary(table_number: int):
    items = get_items_by_table(table_number)

    return {
        "table_number": int(table_number),
        "item_count": len(items),
        "max_item_id": max((item["id"] for item in items), default=0),
    }


def ensure_item_counter():
    """
    Đảm bảo /meta/next_item_id không nhỏ hơn ID lớn nhất đang có.
    Điều này giúp tránh đụng ID nếu Firebase đã có dữ liệu trước đó.
    """
    raw_items = get_all_items_raw()

    max_existing_id = 0
    for item_id, item_data in raw_items.items():
        try:
            candidate = int(
                item_data.get("id", item_id)
                if isinstance(item_data, dict)
                else item_id
            )
            max_existing_id = max(max_existing_id, candidate)
        except (TypeError, ValueError):
            continue

    counter_ref = meta_ref().child("next_item_id")

    def update_counter(current):
        try:
            current_value = int(current or 0)
        except (TypeError, ValueError):
            current_value = 0

        return max(current_value, max_existing_id)

    value = counter_ref.transaction(update_counter)
    print(f"[FIREBASE] next_item_id = {value}")


def reserve_item_ids(count: int) -> list[int]:
    if count <= 0:
        return []

    counter_ref = meta_ref().child("next_item_id")

    def increment(current):
        try:
            current_value = int(current or 0)
        except (TypeError, ValueError):
            current_value = 0

        return current_value + count

    final_value = int(counter_ref.transaction(increment))
    first_value = final_value - count + 1

    return list(range(first_value, final_value + 1))


# =========================================================
# WEBSOCKET MANAGER
# =========================================================

class ConnectionManager:
    def __init__(self):
        self.connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.connections:
            self.connections.remove(websocket)

    async def broadcast(self, data: dict):
        dead_connections = []

        for websocket in list(self.connections):
            try:
                await websocket.send_json(data)
            except Exception:
                dead_connections.append(websocket)

        for websocket in dead_connections:
            self.disconnect(websocket)


manager = ConnectionManager()
fastapi_loop = None
cooking_task = None


def broadcast_from_mqtt_thread(data: dict):
    """MQTT callback chạy ở thread Paho, chuyển coroutine về loop FastAPI."""
    global fastapi_loop

    if fastapi_loop is None or not fastapi_loop.is_running():
        return

    asyncio.run_coroutine_threadsafe(
        manager.broadcast(data),
        fastapi_loop,
    )


# =========================================================
# MQTT / HIVEMQ
# =========================================================

mqtt_client = mqtt.Client(
    mqtt.CallbackAPIVersion.VERSION2,
    client_id="restaurant-fastapi-backend",
    protocol=mqtt.MQTTv311,
)

if MQTT_CONFIGURED:
    mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    mqtt_client.tls_set_context(ssl.create_default_context())


def on_mqtt_connect(client, userdata, flags, reason_code, properties):
    print(f"[MQTT] Kết nối HiveMQ: {reason_code}")

    if reason_code == 0:
        client.subscribe("topic1/status", qos=1)
        client.subscribe("topic2/status", qos=1)
        print("[MQTT] Subscribe: topic1/status, topic2/status")


def on_mqtt_disconnect(client, userdata, disconnect_flags, reason_code, properties):
    print(f"[MQTT] Mất kết nối: {reason_code}")


def on_mqtt_message(client, userdata, message):
    """
    Robot gửi ví dụ:
    {
        "command_id": "AB12CD34",
        "robot": 2,
        "item_id": 25,
        "table": 3,
        "food_name": "Pizza Hải Sản",
        "status": "delivered"
    }
    """
    try:
        payload = json.loads(message.payload.decode("utf-8"))
        print(f"[MQTT] RECEIVE {message.topic}: {payload}")

        if payload.get("status") != "delivered":
            return

        item_id = int(payload["item_id"])
        table_number = int(payload["table"])
        robot_number = int(payload["robot"])
        command_id = str(payload.get("command_id") or "")

        item = get_item(item_id)

        if item is None:
            print(f"[MQTT] Bỏ qua: không còn item_id={item_id}")
            return

        if int(item["table_number"]) != table_number:
            print("[MQTT] Bỏ qua: số bàn không khớp")
            return

        if (
            item["assigned_robot"] is None
            or int(item["assigned_robot"]) != robot_number
        ):
            print("[MQTT] Bỏ qua: robot không khớp")
            return

        expected_command_id = str(item.get("dispatch_command_id") or "")
        if expected_command_id and command_id != expected_command_id:
            print("[MQTT] Bỏ qua: command_id cũ hoặc không hợp lệ")
            return

        # QoS 1 có thể gửi lặp. Nếu đã delivered thì xử lý idempotent.
        if bool(item["delivered"]):
            print(f"[MQTT] item_id={item_id} đã delivered trước đó")
            return

        item_ref(item_id).update(
            {
                "delivered": True,
                "delivery_status": "delivered",
            }
        )

        food_name = item["food_name"]

        print(
            f"[MQTT] ✓ Robot {robot_number} đã giao {food_name} "
            f"(item {item_id}) tới Bàn {table_number}"
        )

        broadcast_from_mqtt_thread(
            {
                "type": "item_delivered",
                "item_id": item_id,
                "table": table_number,
                "food_name": food_name,
                "robot": robot_number,
                "command_id": command_id,
                "delivered": True,
                "delivery_status": "delivered",
            }
        )

    except Exception as error:
        print(f"[MQTT] Lỗi xử lý message: {error}")


mqtt_client.on_connect = on_mqtt_connect
mqtt_client.on_disconnect = on_mqtt_disconnect
mqtt_client.on_message = on_mqtt_message


def mqtt_topic_for_robot(robot_number: int) -> str:
    return f"topic{robot_number}/command"


def publish_robot_command(topic: str, payload: dict):
    if not MQTT_CONFIGURED:
        raise HTTPException(
            status_code=503,
            detail=(
                "HiveMQ chưa được cấu hình. Hãy đặt MQTT_HOST, MQTT_USERNAME "
                "và MQTT_PASSWORD trong biến môi trường."
            ),
        )

    if not mqtt_client.is_connected():
        raise HTTPException(
            status_code=503,
            detail="Backend chưa kết nối được tới HiveMQ Cloud.",
        )

    try:
        info = mqtt_client.publish(
            topic,
            json.dumps(payload, ensure_ascii=False),
            qos=1,
            retain=False,
        )
        info.wait_for_publish(timeout=3.0)

        if not info.is_published():
            raise RuntimeError("MQTT publish timeout")

    except Exception as error:
        raise HTTPException(
            status_code=503,
            detail=f"Không gửi được MQTT tới HiveMQ: {error}",
        ) from error


# =========================================================
# TỰ ĐỘNG CHUYỂN TRẠNG THÁI NẤU 0 -> 1 -> 2
# =========================================================

def parse_created_at(value: str):
    if not value:
        return None

    try:
        normalized = str(value).strip()

        if normalized.endswith("Z"):
            normalized = normalized[:-1] + "+00:00"

        parsed = datetime.fromisoformat(normalized)

        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)

        return parsed.astimezone(timezone.utc)

    except (ValueError, TypeError):
        return None


def update_cooking_statuses_once():
    """
    cooking_status:

    0 = chưa nấu
    1 = đang nấu
    2 = đã nấu xong

    Sau 5 giây:
        0 -> 1

    Sau 10 giây:
        0/1 -> 2
    """

    raw_items = (
        get_all_items_raw()
    )

    now = datetime.now(
        timezone.utc
    )

    changes = []

    firebase_updates = []

    print(
        f"[COOKING] Scan "
        f"{len(raw_items)} items "
        f"at {now.isoformat()}"
    )

    # =========================================
    # DUYỆT MÓN
    # =========================================

    for (
        item_key,
        data
    ) in raw_items.items():

        if not isinstance(
            data,
            dict
        ):
            continue

        # =====================================
        # ID + STATUS
        # =====================================

        try:

            item_id = int(
                data.get("id")
                or
                item_key
            )

            old_status = int(
                data.get(
                    "cooking_status",
                    0
                )
                or
                0
            )

        except (
            TypeError,
            ValueError
        ):

            print(
                "[COOKING] ID/status "
                "không hợp lệ:",
                item_key
            )

            continue

        # =====================================
        # ĐÃ NẤU XONG
        # =====================================

        if old_status >= 2:
            continue

        # =====================================
        # CREATED_AT
        # =====================================

        created_at_raw = (
            data.get(
                "created_at"
            )
        )

        created_at = (
            parse_created_at(
                created_at_raw
            )
        )

        if created_at is None:

            print(
                f"[COOKING] item={item_id}: "
                f"created_at không hợp lệ: "
                f"{created_at_raw}"
            )

            continue

        # =====================================
        # AGE
        # =====================================

        age_seconds = (
            now - created_at
        ).total_seconds()

        if age_seconds < 0:

            print(
                f"[COOKING] item={item_id}: "
                "created_at ở tương lai"
            )

            age_seconds = 0

        print(
            f"[COOKING] "
            f"item={item_id}, "
            f"age={age_seconds:.1f}s, "
            f"status={old_status}"
        )

        # =====================================
        # STATUS MỚI
        # =====================================

        if age_seconds >= 10:

            new_status = 2

        elif age_seconds >= 5:

            new_status = 1

        else:

            new_status = 0

        # Không đổi
        if (
            new_status ==
            old_status
        ):
            continue

        # =====================================
        # LƯU UPDATE
        # =====================================

        firebase_updates.append(
            (
                str(item_key),
                new_status
            )
        )

        changes.append(
            {
                "type":
                    "item_cooking_status",

                "item_id":
                    item_id,

                "table":
                    int(
                        data.get(
                            "table_number",
                            0
                        )
                        or
                        0
                    ),

                "food_name":
                    str(
                        data.get(
                            "food_name",
                            ""
                        )
                    ),

                "cooking_status":
                    new_status,
            }
        )

        print(
            f"[COOKING] "
            f"item={item_id}: "
            f"{old_status} -> "
            f"{new_status}"
        )

    # =========================================
    # GHI FIREBASE
    # =========================================

    for (
        item_key,
        new_status
    ) in firebase_updates:

        order_items_ref() \
            .child(item_key) \
            .update(
                {
                    "cooking_status":
                        new_status
                }
            )

    return changes


async def cooking_status_worker():

    print(
        "[COOKING] Worker started"
    )

    while True:

        try:

            changes = await asyncio.to_thread(
                update_cooking_statuses_once
            )

            for change in changes:

                await manager.broadcast(
                    change
                )

        except asyncio.CancelledError:

            print(
                "[COOKING] Worker stopped"
            )

            raise

        except Exception as error:

            print(
                "[COOKING] Worker error:",
                repr(error)
            )

        await asyncio.sleep(1)


# =========================================================
# FASTAPI LIFESPAN
# =========================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    global fastapi_loop, cooking_task

    init_firebase()
    validate_auth_configuration()
    await asyncio.to_thread(ensure_item_counter)
    await asyncio.to_thread(ensure_initial_admin)

    fastapi_loop = asyncio.get_running_loop()

    if MQTT_CONFIGURED:
        try:
            mqtt_client.connect_async(MQTT_HOST, MQTT_PORT, keepalive=60)
            mqtt_client.loop_start()
            print(f"[MQTT] Đang kết nối {MQTT_HOST}:{MQTT_PORT} ...")
        except Exception as error:
            print(f"[MQTT] Không thể khởi động MQTT: {error}")
    else:
        print("[MQTT] Chưa cấu hình HiveMQ. REST/WebSocket vẫn chạy bình thường.")

    cooking_task = asyncio.create_task(cooking_status_worker())

    try:
        yield
    finally:
        if cooking_task is not None:
            cooking_task.cancel()
            try:
                await cooking_task
            except asyncio.CancelledError:
                pass

        if MQTT_CONFIGURED:
            try:
                mqtt_client.disconnect()
                mqtt_client.loop_stop()
            except Exception:
                pass


# =========================================================
# FASTAPI APP
# =========================================================

app = FastAPI(
    title="Restaurant Order API + Firebase + MQTT + WebSocket",
    version="4.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =========================================================
# AUTH API
# =========================================================

@app.post("/auth/login")
async def login(data: LoginRequest):
    user = await asyncio.to_thread(authenticate_user, data.username, data.password)

    if user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Sai username hoặc password.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token, expires_at = create_access_token(user)

    return {
        "access_token": token,
        "token_type": "bearer",
        "expires_at": expires_at.isoformat(),
        "user": public_user(user),
    }


@app.get("/auth/me")
def auth_me(current_user: dict = Depends(get_current_user)):
    return public_user(current_user)


@app.post("/auth/logout")
async def logout(current_user: dict = Depends(get_current_user)):
    # Invalidate all JWTs issued before this logout for the account.
    await asyncio.to_thread(bump_token_version, str(current_user["id"]))
    return {"message": "Đăng xuất thành công."}


@app.patch("/auth/change-password")
async def change_own_password(
    data: PasswordChangeRequest,
    current_user: dict = Depends(get_current_user),
):
    try:
        password_ok = password_hasher.verify(
            data.current_password,
            str(current_user.get("password_hash") or ""),
        )
    except Exception:
        password_ok = False

    if not password_ok:
        raise HTTPException(status_code=400, detail="Mật khẩu hiện tại không đúng.")

    if data.current_password == data.new_password:
        raise HTTPException(status_code=400, detail="Mật khẩu mới phải khác mật khẩu hiện tại.")

    new_hash = await asyncio.to_thread(password_hasher.hash, data.new_password)
    now_iso = datetime.now(timezone.utc).isoformat()

    await asyncio.to_thread(
        users_ref().child(str(current_user["id"])).update,
        {
            "password_hash": new_hash,
            "updated_at": now_iso,
        },
    )
    await asyncio.to_thread(bump_token_version, str(current_user["id"]))

    return {
        "message": "Đổi mật khẩu thành công. Hãy đăng nhập lại.",
        "logout_required": True,
    }


# =========================================================
# ADMIN USER MANAGEMENT
# Admin chỉ CRUD tài khoản role=manager.
# =========================================================

@app.get("/admin/users")
def list_manager_users(admin_user: dict = Depends(require_admin)):
    data = users_ref().get() or {}
    users = []

    if isinstance(data, dict):
        for user in data.values():
            if isinstance(user, dict) and str(user.get("role") or "") == "manager":
                users.append(public_user(user))

    users.sort(key=lambda item: item["username"])
    return {"users": users}


@app.post("/admin/users", status_code=201)
async def create_manager_user(
    data: ManagerUserCreate,
    admin_user: dict = Depends(require_admin),
):
    user = await asyncio.to_thread(
        create_user_record,
        data.username,
        data.password,
        "manager",
        data.active,
    )
    return {"message": "Tạo tài khoản quản lý thành công.", "user": public_user(user)}


@app.patch("/admin/users/{user_id}")
async def update_manager_user(
    user_id: str,
    data: ManagerUserUpdate,
    admin_user: dict = Depends(require_admin),
):
    user = await asyncio.to_thread(get_user_by_id, user_id)

    if not user or str(user.get("role") or "") != "manager":
        raise HTTPException(status_code=404, detail="Không tìm thấy tài khoản quản lý.")

    changed_security_state = False

    if data.username is not None:
        user = await asyncio.to_thread(change_username, user, data.username)

    updates = {}

    if data.active is not None and bool(user.get("active", True)) != data.active:
        updates["active"] = data.active
        changed_security_state = True

    if data.new_password is not None:
        updates["password_hash"] = await asyncio.to_thread(
            password_hasher.hash,
            data.new_password,
        )
        changed_security_state = True

    if updates:
        updates["updated_at"] = datetime.now(timezone.utc).isoformat()
        await asyncio.to_thread(users_ref().child(user_id).update, updates)

    if changed_security_state:
        await asyncio.to_thread(bump_token_version, user_id)

    updated = await asyncio.to_thread(get_user_by_id, user_id)
    return {"message": "Cập nhật tài khoản thành công.", "user": public_user(updated)}


@app.delete("/admin/users/{user_id}")
async def delete_manager_user(
    user_id: str,
    admin_user: dict = Depends(require_admin),
):
    user = await asyncio.to_thread(get_user_by_id, user_id)

    if not user or str(user.get("role") or "") != "manager":
        raise HTTPException(status_code=404, detail="Không tìm thấy tài khoản quản lý.")

    index_key = username_index_key(str(user.get("username_normalized") or user.get("username") or ""))

    await asyncio.to_thread(
        root_ref().update,
        {
            f"users/{user_id}": None,
            f"username_indexes/{index_key}": None,
        },
    )

    return {"message": "Đã xóa tài khoản quản lý."}


# =========================================================
# API TEST / STATUS
# =========================================================

@app.get("/")
def home():
    return {
        "message": "Restaurant API đang hoạt động",
        "database": "Firebase Realtime Database",
        "total_tables": TOTAL_TABLES,
        "mqtt_configured": MQTT_CONFIGURED,
        "mqtt_connected": mqtt_client.is_connected() if MQTT_CONFIGURED else False,
        "auth": "JWT enabled",
    }


@app.get("/mqtt/status")
def mqtt_status(admin_user: dict = Depends(require_admin)):
    return {
        "configured": MQTT_CONFIGURED,
        "connected": mqtt_client.is_connected() if MQTT_CONFIGURED else False,
        "host": MQTT_HOST if MQTT_CONFIGURED else None,
        "port": MQTT_PORT,
        "topics": {
            "robot1_command": "topic1/command",
            "robot1_status": "topic1/status",
            "robot2_command": "topic2/command",
            "robot2_status": "topic2/status",
        },
    }


# =========================================================
# WEBSOCKET DASHBOARD
# =========================================================

@app.websocket("/ws/dashboard")
async def dashboard_websocket(websocket: WebSocket):
    await websocket.accept()

    try:
        raw_auth = await asyncio.wait_for(websocket.receive_text(), timeout=5.0)
        auth_message = json.loads(raw_auth)

        if auth_message.get("type") != "auth" or not auth_message.get("token"):
            await websocket.close(code=1008, reason="Authentication required")
            return

        token = str(auth_message["token"])

        try:
            user = await asyncio.to_thread(verify_access_token, token)
            token_payload = jwt.decode(
                token,
                JWT_SECRET_KEY,
                algorithms=[JWT_ALGORITHM],
            )
            token_exp = float(token_payload["exp"])
        except (HTTPException, InvalidTokenError, KeyError, TypeError, ValueError):
            await websocket.close(code=1008, reason="Invalid or expired token")
            return

        manager.connections.append(websocket)
        await websocket.send_json(
            {
                "type": "auth_ok",
                "user": public_user(user),
            }
        )

        while True:
            remaining_seconds = token_exp - datetime.now(timezone.utc).timestamp()

            if remaining_seconds <= 0:
                manager.disconnect(websocket)
                await websocket.close(code=1008, reason="Token expired")
                return

            try:
                await asyncio.wait_for(
                    websocket.receive_text(),
                    timeout=min(60.0, remaining_seconds),
                )
            except asyncio.TimeoutError:
                # Kiểm tra lại active/token_version định kỳ và đúng lúc JWT hết hạn.
                try:
                    await asyncio.to_thread(verify_access_token, token)
                except HTTPException:
                    manager.disconnect(websocket)
                    await websocket.close(code=1008, reason="Session expired")
                    return

    except asyncio.TimeoutError:
        try:
            await websocket.close(code=1008, reason="Authentication timeout")
        except Exception:
            pass
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as error:
        print("[WS] dashboard error:", repr(error))
        manager.disconnect(websocket)


# =========================================================
# POST /orders - ĐẶT MÓN
# =========================================================

def create_order_in_firebase(order: OrderCreate):
    order_code = uuid.uuid4().hex[:8].upper()
    item_ids = reserve_item_ids(len(order.foods))
    now_iso = datetime.now(timezone.utc).isoformat()

    firebase_updates = {}
    items = []

    for item_id, food in zip(item_ids, order.foods):
        item_data = {
            "id": item_id,
            "order_code": order_code,
            "table_number": order.tableNumber,
            "food_name": food.name,
            "quantity": food.quantity,
            "unit_price": food.price,
            "delivered": False,
            "note": food.note,
            "cooking_status": 0,
            "assigned_robot": None,
            "robot_dispatched": False,
            "delivery_status": "waiting",
            "dispatch_command_id": None,
            "created_at": now_iso,
        }

        firebase_updates[str(item_id)] = item_data
        items.append(firebase_item_to_dict(item_id, item_data))

    # Ghi toàn bộ món của order trong một update.
    order_items_ref().update(firebase_updates)

    total = sum(item["item_total"] for item in items)
    summary = get_table_summary(order.tableNumber)

    return order_code, items, total, summary


@app.post("/orders", status_code=201)
async def create_order(order: OrderCreate):
    if len(order.foods) == 0:
        raise HTTPException(
            status_code=400,
            detail="Đơn hàng phải có ít nhất một món.",
        )

    if order.tableNumber > TOTAL_TABLES:
        raise HTTPException(
            status_code=400,
            detail=f"Chỉ hỗ trợ Bàn 1 đến Bàn {TOTAL_TABLES}.",
        )

    try:
        order_code, items, total, summary = await asyncio.to_thread(
            create_order_in_firebase,
            order,
        )
    except Exception as error:
        raise HTTPException(
            status_code=500,
            detail=f"Không thể ghi đơn hàng vào Firebase: {error}",
        ) from error

    await manager.broadcast(
        {
            "type": "order_created",
            "table": order.tableNumber,
            "order_code": order_code,
            "new_item_ids": [item["id"] for item in items],
            **summary,
        }
    )

    return {
        "message": "Đặt món thành công",
        "order_code": order_code,
        "table_number": order.tableNumber,
        "items": items,
        "total": total,
    }


# =========================================================
# GET /orders
# =========================================================

@app.get("/orders")
def get_all_orders(current_user: dict = Depends(get_current_user)):
    return get_all_items()


# =========================================================
# GET /orders/tables/status
# =========================================================

@app.get("/orders/tables/status")
def get_table_statuses(current_user: dict = Depends(get_current_user)):
    items = get_all_items()

    by_table = {
        table_number: {
            "table_number": table_number,
            "item_count": 0,
            "max_item_id": 0,
        }
        for table_number in range(1, TOTAL_TABLES + 1)
    }

    for item in items:
        table_number = int(item["table_number"])

        if table_number not in by_table:
            continue

        by_table[table_number]["item_count"] += 1
        by_table[table_number]["max_item_id"] = max(
            by_table[table_number]["max_item_id"],
            int(item["id"]),
        )

    return {
        "tables": [
            by_table[table_number]
            for table_number in range(1, TOTAL_TABLES + 1)
        ]
    }


# =========================================================
# GET /orders/table/{table_number}
# =========================================================

@app.get(
    "/orders/table/{table_number}"
)
async def get_orders_by_table(
    table_number: int,
    current_user: dict = Depends(get_current_user),
):

    if (
        table_number <= 0
        or
        table_number > TOTAL_TABLES
    ):

        raise HTTPException(
            status_code=400,
            detail="Số bàn không hợp lệ."
        )

    # =========================================
    # FALLBACK:
    # mỗi lần frontend đọc dữ liệu
    # thì kiểm tra trạng thái nấu luôn
    # =========================================

    try:

        cooking_changes = (
            await asyncio.to_thread(
                update_cooking_statuses_once
            )
        )

        for change in cooking_changes:

            await manager.broadcast(
                change
            )

    except Exception as error:

        print(
            "[COOKING] GET fallback error:",
            repr(error)
        )

    # =========================================
    # ĐỌC DỮ LIỆU
    # =========================================

    items = await asyncio.to_thread(
        get_items_by_table,
        table_number
    )

    return {
        "table_number":
            table_number,

        "items":
            items,

        "total":
            sum(
                item["item_total"]
                for item in items
            ),
    }


# =========================================================
# GET /orders/table/{table_number}/pending
# =========================================================

@app.get("/orders/table/{table_number}/pending")
def get_pending_orders_by_table(
    table_number: int,
    current_user: dict = Depends(get_current_user),
):
    if table_number <= 0 or table_number > TOTAL_TABLES:
        raise HTTPException(status_code=400, detail="Số bàn không hợp lệ.")

    items = [
        item
        for item in get_items_by_table(table_number)
        if not item["delivered"]
    ]

    return {
        "table_number": table_number,
        "items": items,
    }


# =========================================================
# GET /orders/code/{order_code}
# =========================================================

@app.get("/orders/code/{order_code}")
def get_order_by_code(order_code: str):
    items = get_items_by_order_code(order_code)

    if len(items) == 0:
        raise HTTPException(status_code=404, detail="Không tìm thấy mã đặt món.")

    return {
        "order_code": order_code,
        "table_number": items[0]["table_number"],
        "items": items,
        "total": sum(item["item_total"] for item in items),
    }


# =========================================================
# PATCH /order-items/{item_id}/delivered
# =========================================================

def update_delivery_status_in_firebase(item_id: int, delivered: bool):
    item = get_item(item_id)

    if item is None:
        raise HTTPException(status_code=404, detail="Không tìm thấy món.")

    delivery_status = "delivered" if delivered else (
        "dispatched" if bool(item["robot_dispatched"]) else "waiting"
    )

    item_ref(item_id).update(
        {
            "delivered": bool(delivered),
            "delivery_status": delivery_status,
        }
    )

    return item, delivery_status


@app.patch("/order-items/{item_id}/delivered")
async def update_delivery_status(
    item_id: int,
    data: DeliveryUpdate,
    current_user: dict = Depends(get_current_user),
):
    item, delivery_status = await asyncio.to_thread(
        update_delivery_status_in_firebase,
        item_id,
        data.delivered,
    )

    await manager.broadcast(
        {
            "type": "item_delivery_changed",
            "item_id": item_id,
            "table": int(item["table_number"]),
            "food_name": item["food_name"],
            "delivered": data.delivered,
            "delivery_status": delivery_status,
        }
    )

    return {
        "message": "Cập nhật trạng thái giao món thành công",
        "id": item_id,
        "delivered": data.delivered,
        "delivery_status": delivery_status,
    }


# =========================================================
# PATCH /order-items/{item_id}/quantity
# Giữ tương thích frontend cũ.
# =========================================================

def update_quantity_in_firebase(item_id: int, quantity: int):
    item = get_item(item_id)

    if item is None:
        raise HTTPException(status_code=404, detail="Không tìm thấy món.")

    item_ref(item_id).update({"quantity": quantity})

    summary = get_table_summary(int(item["table_number"]))
    item_total = quantity * int(item["unit_price"])

    return item, summary, item_total


@app.patch("/order-items/{item_id}/quantity")
async def update_quantity(
    item_id: int,
    data: QuantityUpdate,
    current_user: dict = Depends(get_current_user),
):
    item, summary, item_total = await asyncio.to_thread(
        update_quantity_in_firebase,
        item_id,
        data.quantity,
    )

    await manager.broadcast(
        {
            "type": "table_items_updated",
            "table": int(item["table_number"]),
            "reason": "quantity_updated",
            "item_id": item_id,
            **summary,
        }
    )

    return {
        "message": "Cập nhật số lượng thành công",
        "id": item_id,
        "quantity": data.quantity,
        "item_total": item_total,
    }


# =========================================================
# PATCH /orders/table/{table_number}/items
# Update ghi chú + số lượng nhiều món.
# quantity = 0 => DELETE món.
# =========================================================

def update_table_items_in_firebase(table_number: int, edits: list[OrderItemEdit]):
    item_ids = [edit.id for edit in edits]

    if len(item_ids) != len(set(item_ids)):
        raise HTTPException(status_code=400, detail="Danh sách có ID món bị trùng.")

    row_map = {}

    for item_id in item_ids:
        item = get_item(item_id)

        if item is None:
            raise HTTPException(
                status_code=404,
                detail=f"Không tìm thấy món có id {item_id}.",
            )

        if int(item["table_number"]) != table_number:
            raise HTTPException(
                status_code=409,
                detail=f"Món {item['food_name']} không thuộc Bàn {table_number}.",
            )

        row_map[item_id] = item

    firebase_updates = {}
    changes = []
    updated_items = 0
    deleted_items = 0

    for edit in edits:
        item = row_map[edit.id]

        before = {
            "quantity": int(item["quantity"]),
            "note": item["note"] or "",
        }

        after = {
            "quantity": int(edit.quantity),
            "note": edit.note,
        }

        if edit.quantity == 0:
            firebase_updates[str(edit.id)] = None
            action = "deleted"
            deleted_items += 1
        else:
            firebase_updates[f"{edit.id}/quantity"] = int(edit.quantity)
            firebase_updates[f"{edit.id}/note"] = edit.note
            action = "updated"
            updated_items += 1

        changes.append(
            {
                "id": edit.id,
                "food_name": item["food_name"],
                "action": action,
                "before": before,
                "after": after,
            }
        )

    order_items_ref().update(firebase_updates)

    remaining_items = get_items_by_table(table_number)
    total = sum(item["item_total"] for item in remaining_items)
    summary = get_table_summary(table_number)

    return {
        "changes": changes,
        "updated_items": updated_items,
        "deleted_items": deleted_items,
        "remaining_items": remaining_items,
        "total": total,
        "summary": summary,
    }


@app.patch("/orders/table/{table_number}/items")
async def update_table_items(
    table_number: int,
    data: TableItemsUpdate,
    current_user: dict = Depends(get_current_user),
):
    if table_number <= 0 or table_number > TOTAL_TABLES:
        raise HTTPException(status_code=400, detail="Số bàn không hợp lệ.")

    if len(data.items) == 0:
        raise HTTPException(status_code=400, detail="Không có thay đổi để cập nhật.")

    result = await asyncio.to_thread(
        update_table_items_in_firebase,
        table_number,
        data.items,
    )

    await manager.broadcast(
        {
            "type": "table_items_updated",
            "table": table_number,
            "reason": "manager_update",
            "changes": result["changes"],
            **result["summary"],
        }
    )

    return {
        "message": "Cập nhật đơn hàng thành công",
        "table_number": table_number,
        "updated_items": result["updated_items"],
        "deleted_items": result["deleted_items"],
        "changes": result["changes"],
        "items": result["remaining_items"],
        "total": result["total"],
    }


# =========================================================
# PATCH /order-items/{item_id}/robot-dispatch
# Web -> FastAPI -> HiveMQ -> Robot tương ứng
# =========================================================

def prepare_robot_dispatch(item_id: int, robot_number: int, command_id: str):
    item = get_item(item_id)

    if item is None:
        raise HTTPException(status_code=404, detail="Không tìm thấy món.")

    if bool(item["delivered"]):
        raise HTTPException(status_code=409, detail="Món này đã được giao.")

    if int(item["cooking_status"] or 0) < 1:
        raise HTTPException(
            status_code=409,
            detail="Món chưa bắt đầu nấu. Hãy chờ đủ 5 giây trước khi chuyển robot.",
        )

    item_ref(item_id).update(
        {
            "assigned_robot": robot_number,
            "robot_dispatched": True,
            "delivery_status": "dispatched",
            "dispatch_command_id": command_id,
        }
    )

    return item


def mark_robot_dispatch_failed(item_id: int, command_id: str):
    current = get_item(item_id)

    if current is None:
        return

    if current["delivered"]:
        return

    if str(current.get("dispatch_command_id") or "") != str(command_id):
        return

    item_ref(item_id).update(
        {
            "robot_dispatched": False,
            "delivery_status": "failed",
        }
    )


@app.patch("/order-items/{item_id}/robot-dispatch")
async def dispatch_order_to_robot(
    item_id: int,
    data: RobotDispatchUpdate,
    current_user: dict = Depends(get_current_user),
):
    if data.robot not in (1, 2):
        raise HTTPException(status_code=400, detail="Robot không hợp lệ.")

    # Đảm bảo trạng thái nấu vừa được tính trước khi kiểm tra.
    cooking_changes = await asyncio.to_thread(update_cooking_statuses_once)
    for change in cooking_changes:
        await manager.broadcast(change)

    command_id = uuid.uuid4().hex[:8].upper()

    item = await asyncio.to_thread(
        prepare_robot_dispatch,
        item_id,
        data.robot,
        command_id,
    )

    table_number = int(item["table_number"])
    food_name = item["food_name"]
    topic = mqtt_topic_for_robot(data.robot)

    payload = {
        "command_id": command_id,
        "robot": data.robot,
        "item_id": item_id,
        "table": table_number,
        "food_name": food_name,
        "action": "deliver",
    }

    try:
        await asyncio.to_thread(
            publish_robot_command,
            topic,
            payload,
        )
    except HTTPException:
        await asyncio.to_thread(
            mark_robot_dispatch_failed,
            item_id,
            command_id,
        )
        raise

    event = {
        "type": "robot_dispatched",
        "item_id": item_id,
        "table": table_number,
        "food_name": food_name,
        "robot": data.robot,
        "command_id": command_id,
        "topic": topic,
        "robot_dispatched": True,
        "delivery_status": "dispatched",
    }

    await manager.broadcast(event)

    return {
        "message": "Đã gửi lệnh tới robot qua HiveMQ",
        **event,
    }


# =========================================================
# DELETE /orders/table/{table_number}
# Thanh toán + xóa toàn bộ món của bàn.
# Chỉ cho xóa khi tất cả món đã nấu xong và đã giao.
# =========================================================

def checkout_table_in_firebase(table_number: int):
    items = get_items_by_table(table_number)

    if len(items) == 0:
        raise HTTPException(status_code=404, detail="Bàn này không có món để thanh toán.")

    not_cooked = [item for item in items if int(item["cooking_status"]) < 2]
    not_delivered = [item for item in items if not item["delivered"]]

    if not_cooked or not_delivered:
        details = []

        if not_cooked:
            details.append(
                "Chưa nấu xong: "
                + ", ".join(item["food_name"] for item in not_cooked)
            )

        if not_delivered:
            details.append(
                "Chưa giao: "
                + ", ".join(item["food_name"] for item in not_delivered)
            )

        raise HTTPException(status_code=409, detail=" | ".join(details))

    total = sum(item["item_total"] for item in items)
    order_codes = sorted({item["order_code"] for item in items})
    deleted_items = len(items)

    delete_updates = {
        str(item["id"]): None
        for item in items
    }

    order_items_ref().update(delete_updates)

    return {
        "total": total,
        "order_codes": order_codes,
        "deleted_items": deleted_items,
    }


@app.delete("/orders/table/{table_number}")
async def checkout_and_delete_table(
    table_number: int,
    current_user: dict = Depends(get_current_user),
):
    if table_number <= 0 or table_number > TOTAL_TABLES:
        raise HTTPException(status_code=400, detail="Số bàn không hợp lệ.")

    result = await asyncio.to_thread(
        checkout_table_in_firebase,
        table_number,
    )

    await manager.broadcast(
        {
            "type": "table_cleared",
            "table": table_number,
            "item_count": 0,
            "max_item_id": 0,
        }
    )

    return {
        "message": "Thanh toán thành công và đã xóa đơn của bàn",
        "table_number": table_number,
        "total": result["total"],
        "deleted_items": result["deleted_items"],
        "deleted_order_codes": result["order_codes"],
    }

## Debug API
@app.get("/debug/cooking")
async def debug_cooking(admin_user: dict = Depends(require_admin)):

    try:

        changes = (
            await asyncio.to_thread(
                update_cooking_statuses_once
            )
        )

        for change in changes:

            await manager.broadcast(
                change
            )

        items = await asyncio.to_thread(
            get_all_items
        )

        return {
            "success":
                True,

            "changes":
                changes,

            "items":
                items,
        }

    except Exception as error:

        return {
            "success":
                False,

            "error":
                repr(error),
        }