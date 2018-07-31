===========
Radical RPC
===========

    █▀▄ █▀█ █▀▄ ▀█▀ █▀▀ █▀█ █     █▀▄ █▀█ █▀▀
    █▀▄ █▀█ █ █  █  █   █▀█ █     █▀▄ █▀▀ █
    ▀ ▀ ▀ ▀ ▀▀  ▀▀▀ ▀▀▀ ▀ ▀ ▀▀▀   ▀ ▀ ▀   ▀▀▀

.. contents:: Contents

Decription
----------

About
~~~~~

Radical is a RPC library that allows you to have cross-service communication out of the box.

It can serve as a replacement for Celery.

Radical has out-of-the-box integration with Django and also supports asyncio, but can be used without it.

Radical is easily extensible with custom transports and serializers.

Default and recommended transport is Redis.

Default and recommended serializer is Pickle. JSON serializer is also included.

Glossary
~~~~~~~~

============    ==================================================
Term            Meaning
============    ==================================================
Service         A fully autonomous application written with Django,
                Sanic or vanilla Python.
Transport       Module that provides interface to actual network
                communication: for example, a Redis transport.
Serializer      Module that provides interface to data
                serialization and deserialization.
============    ==================================================

Quick start
-----------

Installing
~~~~~~~~~~

    pip install radical-rpc

Using with Django
~~~~~~~~~~~~~~~~~

1. Add Radical to INSTALLED_APPS:

    INSTALLED_APPS = [
        # ...
        'radical',
        # ...
    ]

2. Configure Radical:

    RADICAL_CONFIG = {
        'TRANSPORT_URL': 'redis://redis:6379/0?request_timeout=10',
        'QUEUE_NAME': 'myapp',
        'MODULES': [
            'radical.demo'
        ]
    }

3. Call it anywhere:

    from radical.contrib.django import call_wait, call
    from django.http import JsonResponse

    def some_view(request):
        # Call remote method and wait for it to return result.
        result = call_wait('myapp', 'radical.demo.add', 1300, 37)
        return JsonResponse(dict(result=result))  # Returns {'result': 1337}

    def some_view(request):
        # Call remote method and do not wait for it to finish.
        call('myapp', 'radical.demo.add', 1300, 37)
        return JsonResponse(dict(result='Job was scheduled.'))

4. Start Radical worker:

    ./manage.py radical
