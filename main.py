import asyncio
from typing import List

import nats
import logging

from nats.aio.client import Client
from nats.aio.msg import Msg
from nats.aio.subscription import Subscription


class MsgCollector:
    def __init__(self, nc: Client):
        self.nc = nc
        self.queue = asyncio.Queue()
        self.sub: Subscription | None = None
        self.msgs: List[Msg] = []
        self.task = None

    async def subscribe(self, subject: str):
        if self.sub:
            raise RuntimeError('Already subscribed')
        else:
            self.sub = await self.nc.subscribe(subject)
            logging.info(f'Subscribed to {subject}')
            await self._start()

    async def unsubscribe(self):
        if self.sub:
            await self.sub.unsubscribe()
            logging.info(f'Unsubscribed')
            await self._stop()
            logging.debug(f'Stopped task')
        else:
            raise RuntimeError('Not subscribed')

    async def _start(self) -> None:
        async def pull():
            while True:
                try:
                    logging.debug(f'Waiting for msg')
                    msg = await self.sub.next_msg(timeout=None)
                    logging.info(f'Received {msg}')
                    self.msgs.append(msg)
                except nats.errors.TimeoutError:
                    raise asyncio.CancelledError

        self.task = asyncio.create_task(pull())

    async def _stop(self):
        try:
            self.task.cancel()
            await self.task
        except asyncio.CancelledError:
            pass


async def main():
    nc = await nats.connect()
    logging.info(f'Connected to {nc.connected_url.netloc} ')
    status = MsgCollector(nc)
    await status.subscribe('test.*')
    await asyncio.sleep(1)
    await nc.publish(subject='test.foo', payload=b'foo')
    logging.info(f'Published on test.foo')
    await nc.publish(subject='test.bar', payload=b'bar')
    logging.info(f'Published on test.bar')
    await asyncio.sleep(1)
    await status.unsubscribe()
    await nc.close()
    print([(msg.data.decode()) for msg in status.msgs])


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
