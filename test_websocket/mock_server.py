from websockets import serve  # type: ignore
import asyncio
import json
from typing import Any, List, Set

port = 8765
subs: Set[str] = set()
# the messages each connection received, in order; one list per connection
inbox: List[List[Any]] = []


async def mock_server(websocket):
    received: List[Any] = []
    inbox.append(received)
    await websocket.send(
        '[{"ev":"status","status":"connected","message":"Connected Successfully"}]'
    )
    async for message in websocket:
        message = json.loads(message)
        received.append(message)
        if "action" not in message:
            continue
        if message["action"] == "auth" and message["params"] == "MAX-CONNECTIONS":
            # emulate the server refusing a connection over the account's limit
            await websocket.send(
                '[{"ev":"status","status":"max_connections","message":"Maximum number of websocket connections exceeded."}]'
            )
            await websocket.close(code=1008)
            return
        if message["action"] == "auth":
            await websocket.send(
                '[{"ev":"status","status":"auth_success","message":"authenticated"}]'
            )
        elif message["action"] == "subscribe":
            if message["params"] == "SLOW.X":
                # emulate the server disconnecting a slow consumer
                await websocket.send(
                    '[{"ev":"status","status":"slow_consumer","message":"Disconnected slow consumer."}]'
                )
                await websocket.close(code=1000)
                return
            for p in message["params"].split(","):
                if p == "DROP.X":
                    # emulate the connection failing, right after a trade
                    await websocket.send(
                        '[{"ev":"T","sym":"X","i":"1","x":1,"p":1,"s":1,"t":1,"q":1}]'
                    )
                    await websocket.close(code=1011)
                    return
                if p.startswith("ERR."):
                    # emulate the server rejecting a subscription
                    await websocket.send(
                        json.dumps(
                            [
                                {
                                    "ev": "status",
                                    "status": "error",
                                    "message": "Subscription limit reached for feed.",
                                }
                            ]
                            * 2
                        )
                    )
                    continue
                subs.add(p)
                await websocket.send(
                    json.dumps(
                        [
                            {
                                "ev": "status",
                                "status": "success",
                                "message": f"subscribed to: {p}",
                            }
                        ]
                    )
                )
                await websocket.send(
                    json.dumps(
                        [
                            {
                                "ev": "T",
                                "sym": "AAPL",
                                "i": "5096",
                                "x": 10,
                                "p": 161.87,
                                "s": 300,
                                "c": [14, 41],
                                "t": 1651684192462,
                                "q": 4009402,
                                "z": 3,
                            }
                        ]
                    )
                )
                await websocket.send(
                    json.dumps(
                        [
                            {
                                "ev": "T",
                                "sym": "AMZN",
                                "i": "72815",
                                "x": 12,
                                "p": 161.87,
                                "s": 1,
                                "c": [14, 37, 41],
                                "t": 1651684192536,
                                "q": 4009408,
                                "z": 3,
                            },
                            {
                                "ev": "T",
                                "sym": "AMZN",
                                "i": "799",
                                "x": 4,
                                "p": 161.87,
                                "s": 100,
                                "t": 1651684192717,
                                "q": 4009434,
                                "z": 3,
                            },
                        ]
                    )
                )
        elif message["action"] == "unsubscribe":
            for p in message["params"].split(","):
                subs.discard(p)
                await websocket.send(
                    json.dumps(
                        [
                            {
                                "ev": "status",
                                "status": "success",
                                "message": f"unsubscribed to: {p}",
                            }
                        ]
                    )
                )


def process_request(connection, request):
    if request.path.startswith("/404/"):
        # emulate the server refusing the handshake
        return connection.respond(404, "Not Found\n")


async def run_mock_server():
    async with serve(mock_server, "localhost", port, process_request=process_request):
        await asyncio.Future()  # run forever


if __name__ == "__main__":
    asyncio.run(run_mock_server())
