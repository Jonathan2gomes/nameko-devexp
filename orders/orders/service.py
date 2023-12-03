from nameko.events import EventDispatcher
from nameko.rpc import rpc
from nameko_sqlalchemy import DatabaseSession
from sqlalchemy import func

from orders.exceptions import NotFound
from orders.models import DeclarativeBase, Order, OrderDetail
from orders.schemas import OrderSchema


class OrdersService:
    name = 'orders'

    db = DatabaseSession(DeclarativeBase)
    event_dispatcher = EventDispatcher()

    @rpc
    def get_order(self, order_id):
        order = self.db.query(Order).get(order_id)

        if not order:
            raise NotFound('Order with id {} not found'.format(order_id))

        return OrderSchema().dump(order).data

    @rpc
    def create_order(self, order_details):
        order = Order(
            order_details=[
                OrderDetail(
                    product_id=order_detail['product_id'],
                    price=order_detail['price'],
                    quantity=order_detail['quantity']
                )
                for order_detail in order_details
            ]
        )
        self.db.add(order)
        self.db.commit()

        order = OrderSchema().dump(order).data

        self.event_dispatcher('order_created', {
            'order': order,
        })

        return order

    @rpc
    def update_order(self, order):
        order_details = {
            order_details['id']: order_details
            for order_details in order['order_details']
        }

        order = self.db.query(Order).get(order['id'])

        for order_detail in order.order_details:
            order_detail.price = order_details[order_detail.id]['price']
            order_detail.quantity = order_details[order_detail.id]['quantity']

        self.db.commit()
        return OrderSchema().dump(order).data

    @rpc
    def delete_order(self, order_id):
        order = self.db.query(Order).get(order_id)
        self.db.delete(order)
        self.db.commit()

    @rpc
    def list_orders(self, page=1, per_page=10):
        query = self.db.query(Order).order_by(Order.created_at.asc())
        paginated_orders = query.limit(per_page).offset((page - 1) * per_page).all()

        return [OrderSchema().dump(order).data for order in paginated_orders]

    @rpc
    def get_total_orders(self):
        return self.db.query(func.count(Order.id)).scalar()

    @rpc
    def get_order_by_product_id(self, product_id):
        order = self.db.query(Order).filter(Order.order_details.any(product_id=product_id)).first()

        if not order:
            raise NotFound('Product with id {} is not associated with any order'.format(product_id))

        return OrderSchema().dump(order).data
