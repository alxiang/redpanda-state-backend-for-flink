import argparse
import numpy as np
from kafka import KafkaProducer
import json
from tqdm import tqdm


def generate_vector(vector_size):
    """Generates a vector with numpy and converts to string form"""
    vec = np.random.rand(vector_size)

    return ",".join([str(x) for x in vec])



def produce_vectors(args):
    producer = KafkaProducer(
        bootstrap_servers=f'{args.host}:9192',
        key_serializer=lambda k: json.dumps(k).encode('ascii'),
        value_serializer=lambda v: json.dumps(v).encode('ascii'),
        linger_ms=1000,
    )

    
    for i in tqdm(range(args.num_vectors)):
        vec = generate_vector(args.vector_size)
        producer.send(
            topic="Vector", 
            key=vec,
            value=i,
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('host', type=str, default="10.10.1.1", nargs="?")
    parser.add_argument('num_vectors', type=int, default=100_000, nargs='?')  # number of vectors to produce
    parser.add_argument('vector_size', type=int, default=64, nargs='?')  # vector size
    args = parser.parse_args()

    produce_vectors(args)


if __name__ == "__main__":
    main()