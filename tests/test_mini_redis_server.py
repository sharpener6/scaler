import asyncio
import unittest

import redis.asyncio as redis

HOST, PORT = 'localhost', 6379

class MiniRedisTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.client = redis.Redis(host=HOST, port=PORT, decode_responses=True)
        await self.client.flushdb()

    async def asyncTearDown(self):
        await self.client.close()

    async def test_ping(self):
        resp = await self.client.ping()
        self.assertTrue(resp)

    async def test_echo(self):
        resp = await self.client.echo('hello')
        self.assertEqual(resp, 'hello')

    async def test_set_get(self):
        await self.client.set('key1', 'value1')
        val = await self.client.get('key1')
        self.assertEqual(val, 'value1')

    async def test_del_exists(self):
        await self.client.set('key2', 'val')
        self.assertEqual(await self.client.exists('key2'), 1)
        self.assertEqual(await self.client.delete('key2'), 1)
        self.assertEqual(await self.client.exists('key2'), 0)

    async def test_set_px_expire(self):
        await self.client.set('key3', 'expiring', px=100)
        val = await self.client.get('key3')
        self.assertEqual(val, 'expiring')
        await asyncio.sleep(0.2)
        self.assertIsNone(await self.client.get('key3'))

    async def test_expire_and_ttl(self):
        await self.client.set('key4', 'temp')
        await self.client.expire('key4', 1)
        ttl = await self.client.pttl('key4')
        self.assertTrue(ttl >= 0)
        await asyncio.sleep(1.2)
        self.assertEqual(await self.client.ttl('key4'), -2)

    async def test_invalid_command(self):
        try:
            await self.client.execute_command('FOOBAR')
        except redis.exceptions.ResponseError as e:
            self.assertIn('ERR', str(e))
        else:
            self.fail("Expected error not raised")
