import asyncio
import logging
from typing import List

import nats
from nats.aio.client import Client
from nats.aio.msg import Msg
from nats.aio.subscription import Subscription


class MsgCollector:
    def __init__(self, nc: Client):
        self.nc = nc
        self.sub: Subscription | None = None
        self.msgs: List[Msg] = []
        self.task = None

    async def subscribe(self, subject: str):
        if self.sub:
            raise RuntimeError('Already subscribed')

        self.sub = await self.nc.subscribe(subject)
        logging.info(f'Subscribed to {subject}')
        await self._start()

    async def unsubscribe(self):
        if not self.sub:
            raise RuntimeError('Not subscribed')

        await self.sub.unsubscribe()
        self.sub = None
        logging.info(f'Unsubscribed')
        await self._stop()
        logging.debug(f'Stopped task')

    async def _start(self) -> None:
        async def pull():
            while True:
                try:
                    logging.debug(f'Waiting for msg')
                    msg = await self.sub.next_msg(timeout=None)
                    logging.info(f'Received {msg}')
                    self.msgs.append(msg)
                except nats.errors.TimeoutError:
                    # Can be removed when https://github.com/nats-io/nats.py/pull/378 is merged and released
                    raise asyncio.CancelledError

        self.task = asyncio.create_task(pull())

    async def _stop(self) -> None:
        try:
            self.task.cancel()
            await self.task
        except asyncio.CancelledError:
            pass


async def main():
    nc = await nats.connect()
    logging.info(f'Connected to {nc.connected_url.netloc}')
    collector = MsgCollector(nc)
    await collector.subscribe('test.*')
    await nc.publish(subject='test.foo', payload=b'foo')
    logging.info(f'Published on test.foo')
    await nc.publish(subject='test.bar', payload=b'bar')
    logging.info(f'Published on test.bar')
    await asyncio.sleep(1)
    await collector.unsubscribe()
    await nc.close()
    print([(msg.data.decode()) for msg in collector.msgs])


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
