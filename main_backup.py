import sqlite3
import uuid

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field


# =========================================================
# CẤU HÌNH
# =========================================================

DATABASE_NAME = "restaurant.db"


# =========================================================
# FASTAPI
# =========================================================

app = FastAPI(
    title="Restaurant Order API",
    version="1.0.0"
)


# =========================================================
# CORS
# Cho phép frontend gọi API
# =========================================================

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =========================================================
# MODEL MÓN ĂN FRONTEND GỬI LÊN
# =========================================================

class FoodItem(BaseModel):

    name: str = Field(
        min_length=1,
        max_length=200
    )

    price: int = Field(
        gt=0
    )

    quantity: int = Field(
        gt=0
    )

    note: str = Field(
        default="",
        max_length=500
    )


# =========================================================
# MODEL ĐƠN HÀNG
# =========================================================

class OrderCreate(BaseModel):

    tableNumber: int = Field(
        gt=0
    )

    foods: list[FoodItem]


# =========================================================
# MODEL CẬP NHẬT TRẠNG THÁI GIAO MÓN
# =========================================================

class DeliveryUpdate(BaseModel):

    delivered: bool


# =========================================================
# MODEL CẬP NHẬT SỐ LƯỢNG
# =========================================================

class QuantityUpdate(BaseModel):

    quantity: int = Field(
        gt=0
    )


# =========================================================
# KẾT NỐI SQLITE
# =========================================================

def get_connection():

    conn = sqlite3.connect(
        DATABASE_NAME
    )

    # Cho phép truy cập:
    # row["food_name"]
    # thay vì row[3]
    conn.row_factory = sqlite3.Row

    return conn


# =========================================================
# KHỞI TẠO DATABASE
# =========================================================

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

                created_at TEXT NOT NULL
                    DEFAULT CURRENT_TIMESTAMP,

                CHECK (table_number > 0),

                CHECK (quantity > 0),

                CHECK (unit_price > 0),

                CHECK (delivered IN (0, 1))
            )
            """
        )


        # Index tìm theo bàn nhanh hơn
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_order_items_table_number
            ON order_items(table_number)
            """
        )


        # Index tìm theo mã đặt món
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_order_items_order_code
            ON order_items(order_code)
            """
        )


        conn.commit()

    finally:

        conn.close()


# Tạo database khi server chạy
init_database()


# =========================================================
# HÀM CHUYỂN ROW SQLITE THÀNH JSON
# =========================================================

def row_to_dict(row):

    return {

        "id":
            row["id"],

        "order_code":
            row["order_code"],

        "table_number":
            row["table_number"],

        "food_name":
            row["food_name"],

        "quantity":
            row["quantity"],

        "unit_price":
            row["unit_price"],

        "delivered":
            bool(row["delivered"]),

        "note":
            row["note"],

        "created_at":
            row["created_at"],

        # Không lưu vào DB,
        # chỉ tính khi trả về
        "item_total":
            row["quantity"]
            *
            row["unit_price"]
    }


# =========================================================
# API TEST SERVER
# =========================================================

@app.get("/")
def home():

    return {
        "message":
            "Restaurant API đang hoạt động"
    }


# =========================================================
# API ĐẶT MÓN
# =========================================================
#
# POST /orders
#
# Frontend gửi:
#
# {
#     "tableNumber": 3,
#     "foods": [...]
# }
#
# =========================================================

@app.post(
    "/orders",
    status_code=201
)
def create_order(
    order: OrderCreate
):


    # =====================================================
    # KIỂM TRA CÓ MÓN KHÔNG
    # =====================================================

    if len(order.foods) == 0:

        raise HTTPException(
            status_code=400,
            detail="Đơn hàng phải có ít nhất một món."
        )


    # =====================================================
    # TẠO MÃ ĐẶT MÓN
    # =====================================================
    #
    # Ví dụ:
    # 23A9B17C
    #
    # Các món trong cùng một lần đặt
    # sẽ có cùng order_code.
    #

    order_code = (
        uuid.uuid4()
        .hex[:8]
        .upper()
    )


    conn = get_connection()


    inserted_items = []


    total = 0


    try:

        # =================================================
        # DUYỆT TỪNG MÓN
        # =================================================

        for food in order.foods:


            # ---------------------------------------------
            # TÍNH THÀNH TIỀN
            # ---------------------------------------------

            item_total = (
                food.price
                *
                food.quantity
            )


            total += item_total


            # ---------------------------------------------
            # INSERT SQLITE
            # ---------------------------------------------

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
                    note
                )

                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,

                (
                    order_code,

                    order.tableNumber,

                    food.name,

                    food.quantity,

                    food.price,

                    0,

                    food.note
                )
            )


            # ---------------------------------------------
            # DỮ LIỆU TRẢ VỀ
            # ---------------------------------------------

            inserted_items.append(
                {

                    "id":
                        cursor.lastrowid,

                    "order_code":
                        order_code,

                    "table_number":
                        order.tableNumber,

                    "food_name":
                        food.name,

                    "quantity":
                        food.quantity,

                    "unit_price":
                        food.price,

                    "delivered":
                        False,

                    "note":
                        food.note,

                    "item_total":
                        item_total
                }
            )


        # =================================================
        # LƯU DATABASE
        # =================================================

        conn.commit()


    except Exception:

        conn.rollback()

        raise


    finally:

        conn.close()


    # =====================================================
    # RESPONSE
    # =====================================================

    return {

        "message":
            "Đặt món thành công",

        "order_code":
            order_code,

        "table_number":
            order.tableNumber,

        "items":
            inserted_items,

        "total":
            total
    }


# =========================================================
# API QUERY TOÀN BỘ MÓN
# =========================================================
#
# GET /orders
#
# =========================================================

@app.get("/orders")
def get_all_orders():

    conn = get_connection()

    try:

        rows = conn.execute(
            """
            SELECT
                id,
                order_code,
                table_number,
                food_name,
                quantity,
                unit_price,
                delivered,
                note,
                created_at

            FROM order_items

            ORDER BY id DESC
            """
        ).fetchall()

    finally:

        conn.close()


    return [
        row_to_dict(row)
        for row in rows
    ]


# =========================================================
# API QUERY THEO SỐ BÀN
# =========================================================
#
# Ví dụ:
#
# GET /orders/table/3
#
# → lấy tất cả món của bàn 3
#
# =========================================================

@app.get(
    "/orders/table/{table_number}"
)
def get_orders_by_table(
    table_number: int
):


    if table_number <= 0:

        raise HTTPException(
            status_code=400,
            detail="Số bàn không hợp lệ."
        )


    conn = get_connection()


    try:

        rows = conn.execute(
            """
            SELECT
                id,
                order_code,
                table_number,
                food_name,
                quantity,
                unit_price,
                delivered,
                note,
                created_at

            FROM order_items

            WHERE table_number = ?

            ORDER BY id ASC
            """,

            (
                table_number,
            )
        ).fetchall()

    finally:

        conn.close()


    # =====================================================
    # TÍNH TỔNG TIỀN CỦA BÀN
    # =====================================================

    total = 0


    items = []


    for row in rows:

        item = row_to_dict(row)


        total += (
            item["quantity"]
            *
            item["unit_price"]
        )


        items.append(
            item
        )


    # =====================================================
    # RESPONSE
    # =====================================================

    return {

        "table_number":
            table_number,

        "items":
            items,

        "total":
            total
    }


# =========================================================
# API QUERY MÓN CHƯA GIAO THEO BÀN
# =========================================================
#
# GET /orders/table/3/pending
#
# =========================================================

@app.get(
    "/orders/table/{table_number}/pending"
)
def get_pending_orders_by_table(
    table_number: int
):

    conn = get_connection()


    try:

        rows = conn.execute(
            """
            SELECT
                id,
                order_code,
                table_number,
                food_name,
                quantity,
                unit_price,
                delivered,
                note,
                created_at

            FROM order_items

            WHERE table_number = ?
            AND delivered = 0

            ORDER BY id ASC
            """,

            (
                table_number,
            )
        ).fetchall()

    finally:

        conn.close()


    return {

        "table_number":
            table_number,

        "items": [
            row_to_dict(row)
            for row in rows
        ]
    }


# =========================================================
# API QUERY THEO MÃ ĐẶT MÓN
# =========================================================
#
# GET /orders/code/23A9B17C
#
# =========================================================

@app.get(
    "/orders/code/{order_code}"
)
def get_order_by_code(
    order_code: str
):

    conn = get_connection()


    try:

        rows = conn.execute(
            """
            SELECT
                id,
                order_code,
                table_number,
                food_name,
                quantity,
                unit_price,
                delivered,
                note,
                created_at

            FROM order_items

            WHERE order_code = ?

            ORDER BY id ASC
            """,

            (
                order_code,
            )
        ).fetchall()

    finally:

        conn.close()


    if len(rows) == 0:

        raise HTTPException(
            status_code=404,
            detail="Không tìm thấy mã đặt món."
        )


    items = [
        row_to_dict(row)
        for row in rows
    ]


    total = sum(
        item["quantity"]
        *
        item["unit_price"]

        for item in items
    )


    return {

        "order_code":
            order_code,

        "table_number":
            items[0]["table_number"],

        "items":
            items,

        "total":
            total
    }


# =========================================================
# API CẬP NHẬT TRẠNG THÁI GIAO
# =========================================================
#
# PATCH /order-items/5/delivered
#
# Body:
#
# {
#     "delivered": true
# }
#
# =========================================================

@app.patch(
    "/order-items/{item_id}/delivered"
)
def update_delivery_status(
    item_id: int,
    data: DeliveryUpdate
):

    conn = get_connection()


    try:

        # =================================================
        # KIỂM TRA MÓN TỒN TẠI
        # =================================================

        item = conn.execute(
            """
            SELECT id

            FROM order_items

            WHERE id = ?
            """,

            (
                item_id,
            )
        ).fetchone()


        if item is None:

            raise HTTPException(
                status_code=404,
                detail="Không tìm thấy món."
            )


        # =================================================
        # UPDATE
        # =================================================

        conn.execute(
            """
            UPDATE order_items

            SET delivered = ?

            WHERE id = ?
            """,

            (
                1
                if data.delivered
                else 0,

                item_id
            )
        )


        conn.commit()


    finally:

        conn.close()


    return {

        "message":
            "Cập nhật trạng thái thành công",

        "id":
            item_id,

        "delivered":
            data.delivered
    }


# =========================================================
# API CẬP NHẬT SỐ LƯỢNG
# =========================================================
#
# PATCH /order-items/5/quantity
#
# Body:
#
# {
#     "quantity": 3
# }
#
# =========================================================

@app.patch(
    "/order-items/{item_id}/quantity"
)
def update_quantity(
    item_id: int,
    data: QuantityUpdate
):

    conn = get_connection()


    try:

        item = conn.execute(
            """
            SELECT
                id,
                unit_price

            FROM order_items

            WHERE id = ?
            """,

            (
                item_id,
            )
        ).fetchone()


        if item is None:

            raise HTTPException(
                status_code=404,
                detail="Không tìm thấy món."
            )


        conn.execute(
            """
            UPDATE order_items

            SET quantity = ?

            WHERE id = ?
            """,

            (
                data.quantity,

                item_id
            )
        )


        conn.commit()


        item_total = (
            data.quantity
            *
            item["unit_price"]
        )


    finally:

        conn.close()


    return {

        "message":
            "Cập nhật số lượng thành công",

        "id":
            item_id,

        "quantity":
            data.quantity,

        "item_total":
            item_total
    }