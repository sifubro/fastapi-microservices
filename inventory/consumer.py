from main import redis, Product
import time

key = 'order_completed'
group = 'inventory-group' #name for consumer group

try:
    # Create Redis consumer group (that consumes the payment Order event from redis.xadd in payment/main.py)
    redis.xgroup_create(key, group)
except:
    print('Group already exists!')


while True:
    try:
        # consume the payment order event from payment/main.py
        # {key: '>'} means we want to get ALL the events
        results = redis.xreadgroup(group, key, {key: '>'}, None)

        if results != []:
            # loop throught the events
            for result in results:
                # get the order. obj=Order
                obj = result[1][0][1]
                try:
                    product = Product.get(obj['product_id'])
                    # decrease the quantity of Product after making the order
                    product.quantity = product.quantity - int(obj['quantity'])
                    product.save()
                except:
                    redis.xadd('refund_order', obj, '*')

    except Exception as e:
        print(str(e))
    time.sleep(1)
