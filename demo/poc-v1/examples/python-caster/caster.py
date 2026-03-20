"""
Rune Python Caster — POC v1
连接到 Rune Server，注册 "echo" Rune，等待执行。
"""
import json
import queue
import threading
import grpc

# 生成的 proto 代码
from rune.wire.v1 import rune_pb2
from rune.wire.v1 import rune_pb2_grpc

GRPC_ADDR = "localhost:50070"
CASTER_ID = "python-caster-1"


def echo_handler(input_bytes: bytes) -> bytes:
    """echo Rune: 原样返回输入"""
    return input_bytes


# WorkItem 注册表
RUNE_HANDLERS = {
    "echo": echo_handler,
}


def create_outbound_stream(out_queue: queue.Queue):
    """从队列读消息，yield 给 gRPC stream"""
    while True:
        msg = out_queue.get()
        if msg is None:
            break
        yield msg


def run():
    channel = grpc.insecure_channel(GRPC_ADDR)
    stub = rune_pb2_grpc.RuneServiceStub(channel)

    out_queue = queue.Queue()

    # 发送 Attach 消息
    attach_msg = rune_pb2.SessionMessage(
        attach=rune_pb2.CasterAttach(
            caster_id=CASTER_ID,
            runes=[
                rune_pb2.RuneDeclaration(
                    name="echo",
                    version="1.0.0",
                    description="Echo rune - returns input as-is",
                    gate=rune_pb2.GateConfig(path="/echo"),
                )
            ],
            max_concurrent=10,
        )
    )
    out_queue.put(attach_msg)

    # 双向流
    responses = stub.Session(create_outbound_stream(out_queue))

    print(f"[caster] connected to {GRPC_ADDR}, waiting for tasks...")

    for msg in responses:
        payload = msg.WhichOneof("payload")

        if payload == "attach_ack":
            ack = msg.attach_ack
            if ack.accepted:
                print(f"[caster] attached! caster_id={CASTER_ID}")
            else:
                print(f"[caster] rejected: {ack.reason}")
                break

        elif payload == "execute":
            req = msg.execute
            print(f"[caster] executing rune={req.rune_name} request_id={req.request_id}")

            handler = RUNE_HANDLERS.get(req.rune_name)
            if handler is None:
                # Rune 不存在
                result_msg = rune_pb2.SessionMessage(
                    result=rune_pb2.ExecuteResult(
                        request_id=req.request_id,
                        status=rune_pb2.STATUS_FAILED,
                        error=rune_pb2.ErrorDetail(
                            code="NOT_FOUND",
                            message=f"rune '{req.rune_name}' not registered in this caster",
                        ),
                    )
                )
            else:
                try:
                    output = handler(req.input)
                    result_msg = rune_pb2.SessionMessage(
                        result=rune_pb2.ExecuteResult(
                            request_id=req.request_id,
                            status=rune_pb2.STATUS_COMPLETED,
                            output=output,
                        )
                    )
                    print(f"[caster] completed request_id={req.request_id}")
                except Exception as e:
                    result_msg = rune_pb2.SessionMessage(
                        result=rune_pb2.ExecuteResult(
                            request_id=req.request_id,
                            status=rune_pb2.STATUS_FAILED,
                            error=rune_pb2.ErrorDetail(
                                code="EXECUTION_FAILED",
                                message=str(e),
                            ),
                        )
                    )
                    print(f"[caster] failed request_id={req.request_id}: {e}")

            out_queue.put(result_msg)

        elif payload == "heartbeat":
            pass  # POC: 忽略心跳

    print("[caster] session ended.")
    out_queue.put(None)  # 停止 outbound stream


if __name__ == "__main__":
    run()
