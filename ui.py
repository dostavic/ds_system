import grpc

import p2p_pb2
import p2p_pb2_grpc
from transaction import Transaction


def get_grpc_client(port):
    channel = grpc.insecure_channel(f'{port}')
    return p2p_pb2_grpc.NodeStub(channel)


def connect_to_node(client):
    node_id = input('Enter node_id to connect: ')
    address = input('Enter node address: ')
    node_info = p2p_pb2.NodeInfo(node_id=node_id, address=f'{address}')
    response = client.ConnectToNode(node_info)
    print(f"{response} (node_id: {node_id}, address: {address})")


def get_connected_nodes(client):
    nodes = client.GetConnectedNodes(p2p_pb2.Empty())
    if not nodes.nodes:
        print("Not connected to any")
    else:
        for node in nodes.nodes:
            print(f'Node_id: {node.node_id}, address: {node.address}')


def broadcast_transaction(client, transaction):
    try:
        client.BroadcastTransaction(transaction)
    except grpc.RpcError as e:
        pass


def create_transaction(node_id):
    data = input('Enter data: ')
    transaction = Transaction(node_id=node_id, data=data)
    transaction_grpc = p2p_pb2.Transaction(node_id=node_id, data=data, timestamp=transaction.timestamp.isoformat())
    return transaction_grpc


def get_transactions(client):
    try:
        transactions = client.GetTransactions(p2p_pb2.Empty())
        if transactions.transactions:
            for transaction in transactions.transactions:
                print(f"Node ID: {transaction.node_id}")
                print(f"Timestamp: {transaction.timestamp}")
                print(f"Data: {transaction.data}")
                print("------")
        else:
            print("No transactions found")
    except grpc.RpcError as e:
        print(f'Failed to retrieve transactions: {e}')


def get_blockchain(client):
    try:
        blockchain = client.GetBlockchain(p2p_pb2.Empty())
        print_blockchain(blockchain)
    except grpc.RpcError as e:
        print(f'Failed to retrieve blockchain: {e}')


def print_blockchain(blockchain):
    for block in blockchain.blocks:
        print(f'Block Index: {block.index}')
        print(f'Timestamp: {block.timestamp}')
        print(f'Previous Hash: {block.previous_hash}')
        print(f'Hash: {block.hash}')
        print('Transactions:')
        for transaction in block.transactions:
            print_transaction(transaction)
        print('-' * 30)


def print_transaction(transaction):
    print(f'\tNode ID: {transaction.node_id}')
    print(f'\tData: {transaction.data}')
    print(f'\tTimestamp: {transaction.timestamp}')


def main():
    port = input("Enter address: ")
    client = get_grpc_client(port)
    node_id = input("Enter node_id: ")

    while True:
        command = input("Enter command (connect, list, generate, transactions, blockchain, exit): ")
        if command == "connect":
            connect_to_node(client)
        elif command == "list":
            get_connected_nodes(client)
        elif command == "generate":
            transaction = create_transaction(node_id)
            broadcast_transaction(client, transaction)
        elif command == "transactions":
            get_transactions(client)
        elif command == "blockchain":
            get_blockchain(client)
        elif command == "exit":
            break


if __name__ == "__main__":
    main()
