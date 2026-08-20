import sqlite3
import uuid
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field


# =========================================================
# CẤU HÌNH
# =========================================================
DATABASE_NAME = "restaurant.db"
COOKING_WAIT_SECONDS = 5
COOKING_DONE_SECONDS = 10


# =========================================================
# FASTAPI
# =========================================================
app = FastAPI(
    title="Restaurant Order API",
    version="2.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =========================================================
# MODELS
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


# =========================================================
# DATABASE
# =========================================================
def get_connection():
    conn = sqlite3.connect(DATABASE_NAME)
    conn.row_factory = sqlite3.Row
    return conn


def column_exists(conn, table_name: str, column_name: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return any(row["name"] == column_name for row in rows)


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
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                cooking_status INTEGER NOT NULL DEFAULT 0,
                assigned_robot INTEGER,
                robot_dispatched INTEGER NOT NULL DEFAULT 0,
                CHECK (table_number > 0),
                CHECK (quantity > 0),
                CHECK (unit_price > 0),
                CHECK (delivered IN (0, 1)),
                CHECK (cooking_status IN (0, 1, 2)),
                CHECK (assigned_robot IS NULL OR assigned_robot IN (1, 2)),
                CHECK (robot_dispatched IN (0, 1))
            )
            """
        )

        # Migration cho database cũ đã có bảng order_items.
        if not column_exists(conn, "order_items", "cooking_status"):
            conn.execute(
                "ALTER TABLE order_items ADD COLUMN cooking_status INTEGER NOT NULL DEFAULT 0"
            )

        if not column_exists(conn, "order_items", "assigned_robot"):
            conn.execute(
                "ALTER TABLE order_items ADD COLUMN assigned_robot INTEGER"
            )

        if not column_exists(conn, "order_items", "robot_dispatched"):
            conn.execute(
                "ALTER TABLE order_items ADD COLUMN robot_dispatched INTEGER NOT NULL DEFAULT 0"
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

        conn.commit()
    finally:
        conn.close()


init_database()


# =========================================================
# COOKING STATUS
# 0 = mới nhận / chưa nấu (0-5 giây)
# 1 = đang nấu (5-10 giây)
# 2 = đã nấu xong (>=10 giây)
# =========================================================
def update_cooking_statuses(conn, table_number: Optional[int] = None):
    params: list[int] = []
    where = ""

    if table_number is not None:
        where = "WHERE table_number = ?"
        params.append(table_number)

    conn.execute(
        f"""
        UPDATE order_items
        SET cooking_status = CASE
            WHEN (julianday('now') - julianday(created_at)) * 86400 >= ? THEN 2
            WHEN (julianday('now') - julianday(created_at)) * 86400 >= ? THEN 1
            ELSE 0
        END
        {where}
        """,
        [COOKING_DONE_SECONDS, COOKING_WAIT_SECONDS, *params]
    )
    conn.commit()


def row_to_dict(row):
    return {
        "id": row["id"],
        "order_code": row["order_code"],
        "table_number": row["table_number"],
        "food_name": row["food_name"],
        "quantity": row["quantity"],
        "unit_price": row["unit_price"],
        "delivered": bool(row["delivered"]),
        "note": row["note"],
        "created_at": row["created_at"],
        "cooking_status": int(row["cooking_status"]),
        "assigned_robot": row["assigned_robot"],
        "robot_dispatched": bool(row["robot_dispatched"]),
        "item_total": row["quantity"] * row["unit_price"],
    }


ORDER_COLUMNS = """
    id,
    order_code,
    table_number,
    food_name,
    quantity,
    unit_price,
    delivered,
    note,
    created_at,
    cooking_status,
    assigned_robot,
    robot_dispatched
"""


# =========================================================
# TEST SERVER
# =========================================================
@app.get("/")
def home():
    return {
        "message": "Restaurant API đang hoạt động",
        "server_time": datetime.now(timezone.utc).isoformat(),
    }


# =========================================================
# ĐẶT MÓN
# =========================================================
@app.post("/orders", status_code=201)
def create_order(order: OrderCreate):
    if len(order.foods) == 0:
        raise HTTPException(
            status_code=400,
            detail="Đơn hàng phải có ít nhất một món."
        )

    order_code = uuid.uuid4().hex[:8].upper()
    conn = get_connection()
    inserted_items = []
    total = 0

    try:
        for food in order.foods:
            item_total = food.price * food.quantity
            total += item_total

            cursor = conn.execute(
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
                    robot_dispatched
                )
                VALUES (?, ?, ?, ?, ?, 0, ?, 0, NULL, 0)
                """,
                (
                    order_code,
                    order.tableNumber,
                    food.name,
                    food.quantity,
                    food.price,
                    food.note,
                )
            )

            inserted_items.append(
                {
                    "id": cursor.lastrowid,
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
                    "item_total": item_total,
                }
            )

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return {
        "message": "Đặt món thành công",
        "order_code": order_code,
        "table_number": order.tableNumber,
        "items": inserted_items,
        "total": total,
    }


# =========================================================
# TRẠNG THÁI TẤT CẢ BÀN - DÙNG CHO AUTO REFRESH
# =========================================================
@app.get("/orders/tables/status")
def get_table_statuses():
    conn = get_connection()

    try:
        update_cooking_statuses(conn)

        rows = conn.execute(
            """
            SELECT
                table_number,
                COUNT(*) AS item_count,
                MAX(id) AS max_item_id,
                SUM(CASE WHEN cooking_status < 2 THEN 1 ELSE 0 END) AS unfinished_count,
                SUM(CASE WHEN delivered = 0 THEN 1 ELSE 0 END) AS undelivered_count
            FROM order_items
            GROUP BY table_number
            ORDER BY table_number ASC
            """
        ).fetchall()
    finally:
        conn.close()

    return {
        "tables": [
            {
                "table_number": row["table_number"],
                "item_count": row["item_count"],
                "max_item_id": row["max_item_id"] or 0,
                "unfinished_count": row["unfinished_count"] or 0,
                "undelivered_count": row["undelivered_count"] or 0,
            }
            for row in rows
        ]
    }


# =========================================================
# QUERY TOÀN BỘ
# =========================================================
@app.get("/orders")
def get_all_orders():
    conn = get_connection()

    try:
        update_cooking_statuses(conn)
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
# QUERY THEO BÀN
# =========================================================
@app.get("/orders/table/{table_number}")
def get_orders_by_table(table_number: int):
    if table_number <= 0:
        raise HTTPException(status_code=400, detail="Số bàn không hợp lệ.")

    conn = get_connection()

    try:
        update_cooking_statuses(conn, table_number)
        rows = conn.execute(
            f"""
            SELECT {ORDER_COLUMNS}
            FROM order_items
            WHERE table_number = ?
            ORDER BY id ASC
            """,
            (table_number,)
        ).fetchall()
    finally:
        conn.close()

    items = [row_to_dict(row) for row in rows]
    total = sum(item["item_total"] for item in items)

    return {
        "table_number": table_number,
        "items": items,
        "total": total,
    }


# =========================================================
# QUERY MÓN CHƯA GIAO THEO BÀN
# =========================================================
@app.get("/orders/table/{table_number}/pending")
def get_pending_orders_by_table(table_number: int):
    conn = get_connection()

    try:
        update_cooking_statuses(conn, table_number)
        rows = conn.execute(
            f"""
            SELECT {ORDER_COLUMNS}
            FROM order_items
            WHERE table_number = ?
              AND delivered = 0
            ORDER BY id ASC
            """,
            (table_number,)
        ).fetchall()
    finally:
        conn.close()

    return {
        "table_number": table_number,
        "items": [row_to_dict(row) for row in rows],
    }


# =========================================================
# QUERY THEO MÃ ĐẶT MÓN
# =========================================================
@app.get("/orders/code/{order_code}")
def get_order_by_code(order_code: str):
    conn = get_connection()

    try:
        update_cooking_statuses(conn)
        rows = conn.execute(
            f"""
            SELECT {ORDER_COLUMNS}
            FROM order_items
            WHERE order_code = ?
            ORDER BY id ASC
            """,
            (order_code,)
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
# CẬP NHẬT TRẠNG THÁI GIAO
# =========================================================
@app.patch("/order-items/{item_id}/delivered")
def update_delivery_status(item_id: int, data: DeliveryUpdate):
    conn = get_connection()

    try:
        item = conn.execute(
            "SELECT id FROM order_items WHERE id = ?",
            (item_id,)
        ).fetchone()

        if item is None:
            raise HTTPException(status_code=404, detail="Không tìm thấy món.")

        conn.execute(
            "UPDATE order_items SET delivered = ? WHERE id = ?",
            (1 if data.delivered else 0, item_id)
        )
        conn.commit()
    finally:
        conn.close()

    return {
        "message": "Cập nhật trạng thái giao món thành công",
        "id": item_id,
        "delivered": data.delivered,
    }


# =========================================================
# CẬP NHẬT SỐ LƯỢNG
# =========================================================
@app.patch("/order-items/{item_id}/quantity")
def update_quantity(item_id: int, data: QuantityUpdate):
    conn = get_connection()

    try:
        item = conn.execute(
            "SELECT id, unit_price FROM order_items WHERE id = ?",
            (item_id,)
        ).fetchone()

        if item is None:
            raise HTTPException(status_code=404, detail="Không tìm thấy món.")

        conn.execute(
            "UPDATE order_items SET quantity = ? WHERE id = ?",
            (data.quantity, item_id)
        )
        conn.commit()
        item_total = data.quantity * item["unit_price"]
    finally:
        conn.close()

    return {
        "message": "Cập nhật số lượng thành công",
        "id": item_id,
        "quantity": data.quantity,
        "item_total": item_total,
    }


# =========================================================
# XÁC NHẬN CHUYỂN MÓN CHO ROBOT
# Chỉ được xác nhận sau khi món đã bước sang trạng thái đang nấu.
# =========================================================
@app.patch("/order-items/{item_id}/robot-dispatch")
def dispatch_to_robot(item_id: int, data: RobotDispatchUpdate):
    conn = get_connection()

    try:
        update_cooking_statuses(conn)

        item = conn.execute(
            """
            SELECT id, cooking_status
            FROM order_items
            WHERE id = ?
            """,
            (item_id,)
        ).fetchone()

        if item is None:
            raise HTTPException(status_code=404, detail="Không tìm thấy món.")

        if int(item["cooking_status"]) < 1:
            raise HTTPException(
                status_code=409,
                detail="Món chưa bắt đầu nấu. Vui lòng chờ ít nhất 5 giây."
            )

        conn.execute(
            """
            UPDATE order_items
            SET assigned_robot = ?, robot_dispatched = 1
            WHERE id = ?
            """,
            (data.robot, item_id)
        )
        conn.commit()
    finally:
        conn.close()

    return {
        "message": "Đã xác nhận chuyển món cho robot",
        "id": item_id,
        "assigned_robot": data.robot,
        "robot_dispatched": True,
    }


# =========================================================
# THANH TOÁN = DELETE TOÀN BỘ MÓN CỦA BÀN
# Backend kiểm tra lại để tránh xóa khi còn món chưa nấu/chưa giao.
# =========================================================
@app.delete("/orders/table/{table_number}")
def delete_table_order(table_number: int):
    if table_number <= 0:
        raise HTTPException(status_code=400, detail="Số bàn không hợp lệ.")

    conn = get_connection()

    try:
        update_cooking_statuses(conn, table_number)

        rows = conn.execute(
            f"""
            SELECT {ORDER_COLUMNS}
            FROM order_items
            WHERE table_number = ?
            ORDER BY id ASC
            """,
            (table_number,)
        ).fetchall()

        if not rows:
            raise HTTPException(
                status_code=404,
                detail="Bàn này không có món để thanh toán."
            )

        items = [row_to_dict(row) for row in rows]
        not_cooked = [item for item in items if item["cooking_status"] < 2]
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

            raise HTTPException(
                status_code=409,
                detail="; ".join(details)
            )

        total = sum(item["item_total"] for item in items)
        order_codes = sorted({item["order_code"] for item in items})

        cursor = conn.execute(
            "DELETE FROM order_items WHERE table_number = ?",
            (table_number,)
        )
        conn.commit()

        return {
            "message": "Thanh toán thành công và đã xóa đơn của bàn.",
            "table_number": table_number,
            "deleted_items": cursor.rowcount,
            "deleted_order_codes": order_codes,
            "total": total,
        }
    except HTTPException:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
