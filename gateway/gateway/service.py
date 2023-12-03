import json

from marshmallow import ValidationError
from nameko import config
from nameko.exceptions import BadRequest
from nameko.rpc import RpcProxy
from werkzeug import Response

from gateway.entrypoints import http
from gateway.exceptions import OrderNotFound, ProductNotFound, UnavailableProduct
from gateway.schemas import CreateOrderSchema, GetOrderSchema, ProductSchema


class GatewayService(object):
    """
    Service acts as a gateway to other services over http.
    """

    name = 'gateway'

    orders_rpc = RpcProxy('orders')
    products_rpc = RpcProxy('products')

    @http(
        "GET", "/products/<string:product_id>",
        expected_exceptions=ProductNotFound
    )
    def get_product(self, request, product_id):
        """Gets product by `product_id`
        """
        product = self.products_rpc.get(product_id)
        return Response(
            ProductSchema().dumps(product).data,
            mimetype='application/json'
        )

    @http(
        "DELETE", "/products/<string:product_id>",
        expected_exceptions=(ProductNotFound, BadRequest, UnavailableProduct)
    )
    def delete_product(self, request, product_id):
        try:
            # Check if the product is being used in any order
            self.orders_rpc.get_order_by_product_id(product_id)
            # If the order is found, raise an exception
            raise UnavailableProduct(
                "Product with ID '{}' is associated with an order and cannot be deleted".format(product_id))
        except OrderNotFound:
            try:
                # If the order is not found, proceed with the product deletion
                self.products_rpc.delete(product_id)
            except ProductNotFound:
                # If the product is not found, raise an exception
                raise ProductNotFound("Product with ID '{}' not found".format(product_id))

        return Response(json.dumps({'message': 'Product deleted successfully'}), mimetype='application/json')

    @http(
        "POST", "/products",
        expected_exceptions=(ValidationError, BadRequest)
    )
    def create_product(self, request):
        """Create a new product - product data is posted as json

        Example request ::

            {
                "id": "the_odyssey",
                "title": "The Odyssey",
                "passenger_capacity": 101,
                "maximum_speed": 5,
                "in_stock": 10
            }


        The response contains the new product ID in a json document ::

            {"id": "the_odyssey"}

        """

        schema = ProductSchema(strict=True)

        try:
            # load input data through a schema (for validation)
            # Note - this may raise `ValueError` for invalid json,
            # or `ValidationError` if data is invalid.
            product_data = schema.loads(request.get_data(as_text=True)).data
        except ValueError as exc:
            raise BadRequest("Invalid json: {}".format(exc))

        # Create the product
        self.products_rpc.create(product_data)
        return Response(
            json.dumps({'id': product_data['id']}), mimetype='application/json'
        )

    @http("GET", "/orders/<int:order_id>", expected_exceptions=OrderNotFound)
    def get_order(self, request, order_id):
        """Gets the order details for the order given by `order_id`.

        Enhances the order details with full product details from the
        products-service.
        """
        order = self._get_order(order_id)
        return Response(
            GetOrderSchema().dumps(order).data,
            mimetype='application/json'
        )

    def _get_order(self, order_id):
        # Retrieve order data from the orders service.
        # Note - this may raise a remote exception that has been mapped to
        # raise``OrderNotFound``
        order = self.orders_rpc.get_order(order_id)

        # get the configured image root
        image_root = config['PRODUCT_IMAGE_ROOT']

        # Enhance order details with product and image details.
        for item in order['order_details']:
            product_id = item['product_id']

            item['product'] = self.products_rpc.get(product_id)
            # Construct an image url.
            item['image'] = '{}/{}.jpg'.format(image_root, product_id)

        return order

    @http(
        "POST", "/orders",
        expected_exceptions=(ValidationError, ProductNotFound, BadRequest)
    )
    def create_order(self, request):
        """Create a new order - order data is posted as json

        Example request ::

            {
                "order_details": [
                    {
                        "product_id": "the_odyssey",
                        "price": "99.99",
                        "quantity": 1
                    },
                    {
                        "price": "5.99",
                        "product_id": "the_enigma",
                        "quantity": 2
                    },
                ]
            }


        The response contains the new order ID in a json document ::

            {"id": 1234}

        """

        schema = CreateOrderSchema(strict=True)

        try:
            # load input data through a schema (for validation)
            # Note - this may raise `ValueError` for invalid json,
            # or `ValidationError` if data is invalid.
            order_data = schema.loads(request.get_data(as_text=True)).data
        except ValueError as exc:
            raise BadRequest("Invalid json: {}".format(exc))

        # Create the order
        # Note - this may raise `ProductNotFound`
        id_ = self._create_order(order_data)
        return Response(json.dumps({'id': id_}), mimetype='application/json')

    def _create_order(self, order_data):
        # check order product ids are valid doing a simple check by id
        for item in order_data['order_details']:
            try:
                self.products_rpc.get(item['product_id'])
            except ProductNotFound:
                raise ProductNotFound("Product Id {} not found".format(item['product_id']))

        # Call orders-service to create the order.
        # Dump the data through the schema to ensure the values are serialized
        # correctly.
        serialized_data = CreateOrderSchema().dump(order_data).data
        result = self.orders_rpc.create_order(
            serialized_data['order_details']
        )
        return result['id']

    @http(
        "GET", "/orders",
        expected_exceptions=OrderNotFound
    )
    def list_orders(self, request):
        """
        Gets a paginated list of orders.

        Example request::

            /orders?page=1&per_page=10

        The response contains a paginated list of orders in a JSON document::

            {
                "orders": [...],
                "total": 100
            }
        """

        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 10))

        # Get the configured image root
        image_root = config['PRODUCT_IMAGE_ROOT']

        orders_response = self.orders_rpc.list_orders(page=page, per_page=per_page)
        total_orders = self.orders_rpc.get_total_orders()

        for order in orders_response:
            for item in order['order_details']:
                product_id = item['product_id']

                # Fetch product details from the products service
                item['product'] = self.products_rpc.get(product_id)
                # Construct an image URL
                item['image'] = '{}/{}.jpg'.format(image_root, product_id)

        response_data = {
            "orders": orders_response,
            "total": total_orders
        }

        return Response(
            json.dumps(response_data), mimetype='application/json'
        )
