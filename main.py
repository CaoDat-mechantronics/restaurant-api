import asyncio
import json
import os
import ssl
import sqlite3
import uuid
from contextlib import asynccontextmanager

import paho.mqtt.client as mqtt
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field


# =========================================================
# CẤU HÌNH
# =========================================================

DATABASE_NAME = "restaurant.db"
TOTAL_TABLES = 12

# Có thể thay trực tiếp 4 giá trị mặc định dưới đây,
# hoặc đặt biến môi trường MQTT_HOST/MQTT_PORT/MQTT_USERNAME/MQTT_PASSWORD.
MQTT_HOST = os.getenv("MQTT_HOST")
MQTT_PORT = int(os.getenv("MQTT_PORT", "8883"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")

MQTT_CONFIGURED = (
    MQTT_HOST
    and not MQTT_HOST.startswith("YOUR_")
    and MQTT_USERNAME
    and not MQTT_USERNAME.startswith("YOUR_")
    and MQTT_PASSWORD
    and not MQTT_PASSWORD.startswith("YOUR_")
)


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


# =========================================================
# SQLITE
# =========================================================

def get_connection():
    conn = sqlite3.connect(
        DATABASE_NAME,
        timeout=10,
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    return conn


def table_columns(conn, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {row["name"] for row in rows}


def init_database():
    conn = get_connection()

    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS order_items
            (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                order_code TEXT NOT NULL,
                table_number INTEGER NOT NULL,
                food_name TEXT NOT NULL,
                quantity INTEGER NOT NULL,
                unit_price INTEGER NOT NULL,
                delivered INTEGER NOT NULL DEFAULT 0,
                note TEXT DEFAULT '',
                cooking_status INTEGER NOT NULL DEFAULT 0,
                assigned_robot INTEGER,
                robot_dispatched INTEGER NOT NULL DEFAULT 0,
                delivery_status TEXT NOT NULL DEFAULT 'waiting',
                dispatch_command_id TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,

                CHECK (table_number > 0),
                CHECK (quantity > 0),
                CHECK (unit_price > 0),
                CHECK (delivered IN (0, 1))
            )
            """
        )

        # Migration cho database cũ đã tồn tại.
        columns = table_columns(conn, "order_items")

        migrations = [
            ("cooking_status", "INTEGER NOT NULL DEFAULT 0"),
            ("assigned_robot", "INTEGER"),
            ("robot_dispatched", "INTEGER NOT NULL DEFAULT 0"),
            ("delivery_status", "TEXT NOT NULL DEFAULT 'waiting'"),
            ("dispatch_command_id", "TEXT"),
        ]

        for column_name, definition in migrations:
            if column_name not in columns:
                conn.execute(
                    f"ALTER TABLE order_items ADD COLUMN {column_name} {definition}"
                )

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_order_items_table_number
            ON order_items(table_number)
            """
        )

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_order_items_order_code
            ON order_items(order_code)
            """
        )

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_order_items_dispatch_command_id
            ON order_items(dispatch_command_id)
            """
        )

        conn.commit()

    finally:
        conn.close()


ORDER_COLUMNS = """
    id,
    order_code,
    table_number,
    food_name,
    quantity,
    unit_price,
    delivered,
    note,
    cooking_status,
    assigned_robot,
    robot_dispatched,
    delivery_status,
    dispatch_command_id,
    created_at
"""


def row_to_dict(row):
    return {
        "id": row["id"],
        "order_code": row["order_code"],
        "table_number": row["table_number"],
        "food_name": row["food_name"],
        "quantity": row["quantity"],
        "unit_price": row["unit_price"],
        "delivered": bool(row["delivered"]),
        "note": row["note"] or "",
        "cooking_status": int(row["cooking_status"] or 0),
        "assigned_robot": row["assigned_robot"],
        "robot_dispatched": bool(row["robot_dispatched"]),
        "delivery_status": row["delivery_status"] or "waiting",
        "dispatch_command_id": row["dispatch_command_id"],
        "created_at": row["created_at"],
        "item_total": row["quantity"] * row["unit_price"],
    }


def get_table_summary(conn, table_number: int):
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS item_count,
            COALESCE(MAX(id), 0) AS max_item_id
        FROM order_items
        WHERE table_number = ?
        """,
        (table_number,),
    ).fetchone()

    return {
        "table_number": table_number,
        "item_count": int(row["item_count"] or 0),
        "max_item_id": int(row["max_item_id"] or 0),
    }


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
    """MQTT callback chạy ở thread của Paho, nên chuyển coroutine về loop FastAPI."""
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

        conn = get_connection()

        try:
            item = conn.execute(
                f"""
                SELECT {ORDER_COLUMNS}
                FROM order_items
                WHERE id = ?
                """,
                (item_id,),
            ).fetchone()

            if item is None:
                print(f"[MQTT] Bỏ qua: không còn item_id={item_id}")
                return

            if int(item["table_number"]) != table_number:
                print("[MQTT] Bỏ qua: số bàn không khớp")
                return

            if item["assigned_robot"] is None or int(item["assigned_robot"]) != robot_number:
                print("[MQTT] Bỏ qua: robot không khớp")
                return

            expected_command_id = str(item["dispatch_command_id"] or "")
            if expected_command_id and command_id != expected_command_id:
                print("[MQTT] Bỏ qua: command_id cũ hoặc không hợp lệ")
                return

            # QoS 1 có thể gửi lặp. Nếu đã delivered thì xử lý idempotent.
            if bool(item["delivered"]):
                print(f"[MQTT] item_id={item_id} đã delivered trước đó")
                return

            conn.execute(
                """
                UPDATE order_items
                SET
                    delivered = 1,
                    delivery_status = 'delivered'
                WHERE id = ?
                """,
                (item_id,),
            )
            conn.commit()

            food_name = item["food_name"]

        finally:
            conn.close()

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
                "và MQTT_PASSWORD trong main.py hoặc biến môi trường."
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

def update_cooking_statuses_once():
    """
    0 - dưới 5 giây: chưa nấu
    1 - từ 5 đến dưới 10 giây: đang nấu
    2 - từ 10 giây trở lên: đã nấu

    Trả về các item vừa đổi trạng thái để push WebSocket.
    """
    conn = get_connection()
    changes = []

    try:
        rows = conn.execute(
            """
            SELECT
                id,
                table_number,
                food_name,
                cooking_status,
                CAST(strftime('%s', 'now') AS INTEGER)
                    - CAST(strftime('%s', created_at) AS INTEGER) AS age_seconds
            FROM order_items
            WHERE cooking_status < 2
            """
        ).fetchall()

        for row in rows:
            age_seconds = max(0, int(row["age_seconds"] or 0))
            old_status = int(row["cooking_status"] or 0)

            if age_seconds >= 10:
                new_status = 2
            elif age_seconds >= 5:
                new_status = 1
            else:
                new_status = 0

            if new_status == old_status:
                continue

            conn.execute(
                """
                UPDATE order_items
                SET cooking_status = ?
                WHERE id = ?
                """,
                (new_status, row["id"]),
            )

            changes.append(
                {
                    "type": "item_cooking_status",
                    "item_id": int(row["id"]),
                    "table": int(row["table_number"]),
                    "food_name": row["food_name"],
                    "cooking_status": new_status,
                }
            )

        if changes:
            conn.commit()

    finally:
        conn.close()

    return changes


async def cooking_status_worker():
    while True:
        try:
            changes = await asyncio.to_thread(update_cooking_statuses_once)

            for change in changes:
                await manager.broadcast(change)

        except asyncio.CancelledError:
            raise
        except Exception as error:
            print(f"[COOKING] Worker error: {error}")

        await asyncio.sleep(1)


# =========================================================
# FASTAPI LIFESPAN
# =========================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    global fastapi_loop, cooking_task

    init_database()
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
    title="Restaurant Order API + MQTT + WebSocket",
    version="3.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =========================================================
# API TEST / STATUS
# =========================================================

@app.get("/")
def home():
    return {
        "message": "Restaurant API đang hoạt động",
        "mqtt_configured": MQTT_CONFIGURED,
        "mqtt_connected": mqtt_client.is_connected() if MQTT_CONFIGURED else False,
    }


@app.get("/mqtt/status")
def mqtt_status():
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
    await manager.connect(websocket)

    try:
        # Client có thể không gửi gì. receive_text() chỉ giữ endpoint sống
        # và giúp phát hiện disconnect.
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception:
        manager.disconnect(websocket)


# =========================================================
# POST /orders - ĐẶT MÓN
# =========================================================

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

    order_code = uuid.uuid4().hex[:8].upper()
    conn = get_connection()

    try:
        for food in order.foods:
            conn.execute(
                """
                INSERT INTO order_items
                (
                    order_code,
                    table_number,
                    food_name,
                    quantity,
                    unit_price,
                    delivered,
                    note,
                    cooking_status,
                    assigned_robot,
                    robot_dispatched,
                    delivery_status,
                    dispatch_command_id
                )
                VALUES (?, ?, ?, ?, ?, 0, ?, 0, NULL, 0, 'waiting', NULL)
                """,
                (
                    order_code,
                    order.tableNumber,
                    food.name,
                    food.quantity,
                    food.price,
                    food.note,
                ),
            )

        conn.commit()

        rows = conn.execute(
            f"""
            SELECT {ORDER_COLUMNS}
            FROM order_items
            WHERE order_code = ?
            ORDER BY id ASC
            """,
            (order_code,),
        ).fetchall()

        items = [row_to_dict(row) for row in rows]
        total = sum(item["item_total"] for item in items)
        summary = get_table_summary(conn, order.tableNumber)

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

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
def get_all_orders():
    conn = get_connection()

    try:
        rows = conn.execute(
            f"""
            SELECT {ORDER_COLUMNS}
            FROM order_items
            ORDER BY id DESC
            """
        ).fetchall()
    finally:
        conn.close()

    return [row_to_dict(row) for row in rows]


# =========================================================
# GET /orders/tables/status
# =========================================================

@app.get("/orders/tables/status")
def get_table_statuses():
    conn = get_connection()

    try:
        rows = conn.execute(
            """
            SELECT
                table_number,
                COUNT(*) AS item_count,
                COALESCE(MAX(id), 0) AS max_item_id
            FROM order_items
            GROUP BY table_number
            """
        ).fetchall()

        by_table = {
            int(row["table_number"]): {
                "table_number": int(row["table_number"]),
                "item_count": int(row["item_count"] or 0),
                "max_item_id": int(row["max_item_id"] or 0),
            }
            for row in rows
        }

        return {
            "tables": [
                by_table.get(
                    table_number,
                    {
                        "table_number": table_number,
                        "item_count": 0,
                        "max_item_id": 0,
                    },
                )
                for table_number in range(1, TOTAL_TABLES + 1)
            ]
        }

    finally:
        conn.close()


# =========================================================
# GET /orders/table/{table_number}
# =========================================================

@app.get("/orders/table/{table_number}")
def get_orders_by_table(table_number: int):
    if table_number <= 0 or table_number > TOTAL_TABLES:
        raise HTTPException(status_code=400, detail="Số bàn không hợp lệ.")

    conn = get_connection()

    try:
        rows = conn.execute(
            f"""
            SELECT {ORDER_COLUMNS}
            FROM order_items
            WHERE table_number = ?
            ORDER BY id ASC
            """,
            (table_number,),
        ).fetchall()
    finally:
        conn.close()

    items = [row_to_dict(row) for row in rows]

    return {
        "table_number": table_number,
        "items": items,
        "total": sum(item["item_total"] for item in items),
    }


# =========================================================
# GET /orders/table/{table_number}/pending
# =========================================================

@app.get("/orders/table/{table_number}/pending")
def get_pending_orders_by_table(table_number: int):
    conn = get_connection()

    try:
        rows = conn.execute(
            f"""
            SELECT {ORDER_COLUMNS}
            FROM order_items
            WHERE table_number = ?
              AND delivered = 0
            ORDER BY id ASC
            """,
            (table_number,),
        ).fetchall()
    finally:
        conn.close()

    return {
        "table_number": table_number,
        "items": [row_to_dict(row) for row in rows],
    }


# =========================================================
# GET /orders/code/{order_code}
# =========================================================

@app.get("/orders/code/{order_code}")
def get_order_by_code(order_code: str):
    conn = get_connection()

    try:
        rows = conn.execute(
            f"""
            SELECT {ORDER_COLUMNS}
            FROM order_items
            WHERE order_code = ?
            ORDER BY id ASC
            """,
            (order_code,),
        ).fetchall()
    finally:
        conn.close()

    if len(rows) == 0:
        raise HTTPException(status_code=404, detail="Không tìm thấy mã đặt món.")

    items = [row_to_dict(row) for row in rows]

    return {
        "order_code": order_code,
        "table_number": items[0]["table_number"],
        "items": items,
        "total": sum(item["item_total"] for item in items),
    }


# =========================================================
# PATCH /order-items/{item_id}/delivered
# Cho phép đánh dấu thủ công khi cần.
# =========================================================

@app.patch("/order-items/{item_id}/delivered")
async def update_delivery_status(item_id: int, data: DeliveryUpdate):
    conn = get_connection()

    try:
        item = conn.execute(
            f"""
            SELECT {ORDER_COLUMNS}
            FROM order_items
            WHERE id = ?
            """,
            (item_id,),
        ).fetchone()

        if item is None:
            raise HTTPException(status_code=404, detail="Không tìm thấy món.")

        delivery_status = "delivered" if data.delivered else (
            "dispatched" if bool(item["robot_dispatched"]) else "waiting"
        )

        conn.execute(
            """
            UPDATE order_items
            SET
                delivered = ?,
                delivery_status = ?
            WHERE id = ?
            """,
            (1 if data.delivered else 0, delivery_status, item_id),
        )
        conn.commit()

        table_number = int(item["table_number"])
        food_name = item["food_name"]

    finally:
        conn.close()

    await manager.broadcast(
        {
            "type": "item_delivery_changed",
            "item_id": item_id,
            "table": table_number,
            "food_name": food_name,
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
# Giữ lại để tương thích frontend cũ.
# =========================================================

@app.patch("/order-items/{item_id}/quantity")
async def update_quantity(item_id: int, data: QuantityUpdate):
    conn = get_connection()

    try:
        item = conn.execute(
            """
            SELECT id, table_number, unit_price, food_name
            FROM order_items
            WHERE id = ?
            """,
            (item_id,),
        ).fetchone()

        if item is None:
            raise HTTPException(status_code=404, detail="Không tìm thấy món.")

        conn.execute(
            """
            UPDATE order_items
            SET quantity = ?
            WHERE id = ?
            """,
            (data.quantity, item_id),
        )
        conn.commit()

        summary = get_table_summary(conn, int(item["table_number"]))
        item_total = data.quantity * int(item["unit_price"])

    finally:
        conn.close()

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

@app.patch("/orders/table/{table_number}/items")
async def update_table_items(table_number: int, data: TableItemsUpdate):
    if table_number <= 0 or table_number > TOTAL_TABLES:
        raise HTTPException(status_code=400, detail="Số bàn không hợp lệ.")

    if len(data.items) == 0:
        raise HTTPException(status_code=400, detail="Không có thay đổi để cập nhật.")

    item_ids = [item.id for item in data.items]
    if len(item_ids) != len(set(item_ids)):
        raise HTTPException(status_code=400, detail="Danh sách có ID món bị trùng.")

    conn = get_connection()

    try:
        placeholders = ",".join("?" for _ in item_ids)
        rows = conn.execute(
            f"""
            SELECT {ORDER_COLUMNS}
            FROM order_items
            WHERE id IN ({placeholders})
            """,
            item_ids,
        ).fetchall()

        row_map = {int(row["id"]): row for row in rows}

        for edit in data.items:
            row = row_map.get(edit.id)

            if row is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"Không tìm thấy món có id {edit.id}.",
                )

            if int(row["table_number"]) != table_number:
                raise HTTPException(
                    status_code=409,
                    detail=f"Món {row['food_name']} không thuộc Bàn {table_number}.",
                )

        changes = []
        updated_items = 0
        deleted_items = 0

        for edit in data.items:
            row = row_map[edit.id]

            before = {
                "quantity": int(row["quantity"]),
                "note": row["note"] or "",
            }

            after = {
                "quantity": int(edit.quantity),
                "note": edit.note,
            }

            if edit.quantity == 0:
                conn.execute(
                    """
                    DELETE FROM order_items
                    WHERE id = ? AND table_number = ?
                    """,
                    (edit.id, table_number),
                )
                action = "deleted"
                deleted_items += 1
            else:
                conn.execute(
                    """
                    UPDATE order_items
                    SET
                        quantity = ?,
                        note = ?
                    WHERE id = ? AND table_number = ?
                    """,
                    (edit.quantity, edit.note, edit.id, table_number),
                )
                action = "updated"
                updated_items += 1

            changes.append(
                {
                    "id": edit.id,
                    "food_name": row["food_name"],
                    "action": action,
                    "before": before,
                    "after": after,
                }
            )

        conn.commit()

        remaining_rows = conn.execute(
            f"""
            SELECT {ORDER_COLUMNS}
            FROM order_items
            WHERE table_number = ?
            ORDER BY id ASC
            """,
            (table_number,),
        ).fetchall()

        remaining_items = [row_to_dict(row) for row in remaining_rows]
        total = sum(item["item_total"] for item in remaining_items)
        summary = get_table_summary(conn, table_number)

    except HTTPException:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    await manager.broadcast(
        {
            "type": "table_items_updated",
            "table": table_number,
            "reason": "manager_update",
            "changes": changes,
            **summary,
        }
    )

    return {
        "message": "Cập nhật đơn hàng thành công",
        "table_number": table_number,
        "updated_items": updated_items,
        "deleted_items": deleted_items,
        "changes": changes,
        "items": remaining_items,
        "total": total,
    }


# =========================================================
# PATCH /order-items/{item_id}/robot-dispatch
# Web -> FastAPI -> HiveMQ -> Robot tương ứng
# =========================================================

@app.patch("/order-items/{item_id}/robot-dispatch")
async def dispatch_order_to_robot(item_id: int, data: RobotDispatchUpdate):
    if data.robot not in (1, 2):
        raise HTTPException(status_code=400, detail="Robot không hợp lệ.")

    # Đảm bảo trạng thái nấu vừa được tính trước khi kiểm tra.
    cooking_changes = await asyncio.to_thread(update_cooking_statuses_once)
    for change in cooking_changes:
        await manager.broadcast(change)

    conn = get_connection()
    command_id = uuid.uuid4().hex[:8].upper()

    try:
        item = conn.execute(
            f"""
            SELECT {ORDER_COLUMNS}
            FROM order_items
            WHERE id = ?
            """,
            (item_id,),
        ).fetchone()

        if item is None:
            raise HTTPException(status_code=404, detail="Không tìm thấy món.")

        if bool(item["delivered"]):
            raise HTTPException(status_code=409, detail="Món này đã được giao.")

        if int(item["cooking_status"] or 0) < 1:
            raise HTTPException(
                status_code=409,
                detail="Món chưa bắt đầu nấu. Hãy chờ đủ 5 giây trước khi chuyển robot.",
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

        # Ghi command vào DB trước để ACK từ robot có thể được xác thực ngay.
        conn.execute(
            """
            UPDATE order_items
            SET
                assigned_robot = ?,
                robot_dispatched = 1,
                delivery_status = 'dispatched',
                dispatch_command_id = ?
            WHERE id = ?
            """,
            (data.robot, command_id, item_id),
        )
        conn.commit()

        try:
            publish_robot_command(topic, payload)
        except HTTPException:
            # Nếu publish thất bại, đánh dấu failed để người dùng có thể gửi lại.
            conn.execute(
                """
                UPDATE order_items
                SET
                    robot_dispatched = 0,
                    delivery_status = 'failed'
                WHERE id = ?
                  AND delivered = 0
                  AND dispatch_command_id = ?
                """,
                (item_id, command_id),
            )
            conn.commit()
            raise

    except HTTPException:
        conn.rollback()
        raise
    except Exception as error:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(error)) from error
    finally:
        conn.close()

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

@app.delete("/orders/table/{table_number}")
async def checkout_and_delete_table(table_number: int):
    if table_number <= 0 or table_number > TOTAL_TABLES:
        raise HTTPException(status_code=400, detail="Số bàn không hợp lệ.")

    conn = get_connection()

    try:
        rows = conn.execute(
            f"""
            SELECT {ORDER_COLUMNS}
            FROM order_items
            WHERE table_number = ?
            ORDER BY id ASC
            """,
            (table_number,),
        ).fetchall()

        if len(rows) == 0:
            raise HTTPException(status_code=404, detail="Bàn này không có món để thanh toán.")

        items = [row_to_dict(row) for row in rows]

        not_cooked = [item for item in items if int(item["cooking_status"]) < 2]
        not_delivered = [item for item in items if not item["delivered"]]

        if not_cooked or not_delivered:
            details = []

            if not_cooked:
                details.append(
                    "Chưa nấu xong: " + ", ".join(item["food_name"] for item in not_cooked)
                )

            if not_delivered:
                details.append(
                    "Chưa giao: " + ", ".join(item["food_name"] for item in not_delivered)
                )

            raise HTTPException(status_code=409, detail=" | ".join(details))

        total = sum(item["item_total"] for item in items)
        order_codes = sorted({item["order_code"] for item in items})
        deleted_items = len(items)

        conn.execute(
            """
            DELETE FROM order_items
            WHERE table_number = ?
            """,
            (table_number,),
        )
        conn.commit()

    except HTTPException:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

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
        "total": total,
        "deleted_items": deleted_items,
        "deleted_order_codes": order_codes,
    }
