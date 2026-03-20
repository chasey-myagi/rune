"""
Rune Python Caster — POC v2
支持并发执行 + 心跳 + cancel 处理
"""
import json
import time
import queue
import threading
import grpc

from rune.wire.v1 import rune_pb2
from rune.wire.v1 import rune_pb2_grpc

GRPC_ADDR = "localhost:50070"
CASTER_ID = "python-caster-1"
HEARTBEAT_INTERVAL = 10  # seconds


def echo_handler(input_bytes: bytes) -> bytes:
    """echo: 原样返回"""
    return input_bytes


def slow_handler(input_bytes: bytes) -> bytes:
    """slow: 模拟慢任务（3 秒）"""
    time.sleep(3)
    data = json.loads(input_bytes)
    data["slow"] = True
    return json.dumps(data).encode()


RUNE_HANDLERS = {
    "echo": echo_handler,
    "slow": slow_handler,
}

# 被取消的 request_id 集合
cancelled_requests = set()


def create_outbound_stream(out_queue: queue.Queue):
    while True:
        msg = out_queue.get()
        if msg is None:
            break
        yield msg


def heartbeat_sender(out_queue: queue.Queue, stop_event: threading.Event):
    """后台线程：定期发心跳"""
    while not stop_event.is_set():
        msg = rune_pb2.SessionMessage(
            heartbeat=rune_pb2.Heartbeat(
                timestamp_ms=int(time.time() * 1000)
            )
        )
        out_queue.put(msg)
        stop_event.wait(HEARTBEAT_INTERVAL)


def execute_in_thread(req, out_queue):
    """在线程中执行 rune handler（支持并发）"""
    handler = RUNE_HANDLERS.get(req.rune_name)
    if handler is None:
        result_msg = rune_pb2.SessionMessage(
            result=rune_pb2.ExecuteResult(
                request_id=req.request_id,
                status=rune_pb2.STATUS_FAILED,
                error=rune_pb2.ErrorDetail(code="NOT_FOUND", message=f"rune '{req.rune_name}' not found"),
            )
        )
    else:
        try:
            output = handler(req.input)
            # 执行完检查是否已被取消
            if req.request_id in cancelled_requests:
                cancelled_requests.discard(req.request_id)
                print(f"[caster] request {req.request_id} was cancelled during execution, discarding result")
                return
            result_msg = rune_pb2.SessionMessage(
                result=rune_pb2.ExecuteResult(
                    request_id=req.request_id,
                    status=rune_pb2.STATUS_COMPLETED,
                    output=output,
                )
            )
            print(f"[caster] completed {req.rune_name} request_id={req.request_id}")
        except Exception as e:
            result_msg = rune_pb2.SessionMessage(
                result=rune_pb2.ExecuteResult(
                    request_id=req.request_id,
                    status=rune_pb2.STATUS_FAILED,
                    error=rune_pb2.ErrorDetail(code="EXECUTION_FAILED", message=str(e)),
                )
            )
            print(f"[caster] failed {req.rune_name}: {e}")

    out_queue.put(result_msg)


def run():
    channel = grpc.insecure_channel(GRPC_ADDR)
    stub = rune_pb2_grpc.RuneServiceStub(channel)
    out_queue = queue.Queue()
    stop_event = threading.Event()

    # 发 Attach
    attach_msg = rune_pb2.SessionMessage(
        attach=rune_pb2.CasterAttach(
            caster_id=CASTER_ID,
            runes=[
                rune_pb2.RuneDeclaration(
                    name="echo", version="1.0.0", description="Echo rune",
                    gate=rune_pb2.GateConfig(path="/echo"),
                ),
                rune_pb2.RuneDeclaration(
                    name="slow", version="1.0.0", description="Slow rune (3s delay)",
                    gate=rune_pb2.GateConfig(path="/slow"),
                ),
            ],
            max_concurrent=10,
        )
    )
    out_queue.put(attach_msg)

    # 启动心跳线程
    hb_thread = threading.Thread(target=heartbeat_sender, args=(out_queue, stop_event), daemon=True)
    hb_thread.start()

    # 双向流
    responses = stub.Session(create_outbound_stream(out_queue))
    print(f"[caster] connected to {GRPC_ADDR}")

    for msg in responses:
        payload = msg.WhichOneof("payload")

        if payload == "attach_ack":
            if msg.attach_ack.accepted:
                print(f"[caster] attached! runes: echo, slow")
            else:
                print(f"[caster] rejected: {msg.attach_ack.reason}")
                break

        elif payload == "execute":
            req = msg.execute
            print(f"[caster] dispatching {req.rune_name} request_id={req.request_id}")
            # 并发执行：每个请求开一个线程
            t = threading.Thread(target=execute_in_thread, args=(req, out_queue), daemon=True)
            t.start()

        elif payload == "cancel":
            req_id = msg.cancel.request_id
            reason = msg.cancel.reason
            print(f"[caster] cancel requested: {req_id} reason={reason}")
            cancelled_requests.add(req_id)

        elif payload == "heartbeat":
            pass  # 收到 server 心跳，确认连接存活

    stop_event.set()
    out_queue.put(None)
    print("[caster] session ended.")


if __name__ == "__main__":
    run()
