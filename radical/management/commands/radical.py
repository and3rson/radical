import asyncio

from django.conf import settings
from django.core.management.base import BaseCommand

from radical.worker import Worker


class Command(BaseCommand):
    def handle(self, *args, **kwargs):
        self.worker = Worker(
            settings.RADICAL_CONFIG['TRANSPORT_URL'],
            queue_name=settings.RADICAL_CONFIG.get('QUEUE_NAME'),
            transport=settings.RADICAL_CONFIG.get('TRANSPORT'),
            serializer=settings.RADICAL_CONFIG.get('SERIALIZER'),
            loop=asyncio.get_event_loop()
        )
        self.worker.discover(settings.RADICAL_CONFIG['MODULES'])
        for key, value in self.worker.methods.items():
            print(f'  - {key} -> {value}')
        self.worker.run_until_complete()
