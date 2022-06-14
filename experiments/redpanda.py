import subprocess

def create_topic(topic: str):
    print(f"Creating redpanda topic: {topic}")

    output = subprocess.run([
        'rpk',
        'topic',
        'create',
        topic,
        '-c',
        'message.timestamp.type=LogAppendTime'
    ], capture_output=True)

    return output


def delete_topic(topic: str):
    print(f"Deleting redpanda topic: {topic}")

    output = subprocess.run([
        'rpk',
        'topic',
        'delete',
        topic
    ], capture_output=True)

    return output