import threading
import time
from datetime import datetime

import grpc
from concurrent import futures

import p2p_pb2
import p2p_pb2_grpc
from block import Block
from status import Status, State
from transaction import Transaction


class Node(p2p_pb2_grpc.NodeServicer):
    def __init__(self, node_id, address):
        self.node_id = node_id
        self.address = address
        self.connected_nodes = []
        self.heartbeat_thread = threading.Thread(target=self.heartbeat_loop)
        self.heartbeat_thread.start()
        self.transactions_file = f'transactions_file_node_id_{self.node_id}'
        self.transactions = Transaction.load_from_file(self.transactions_file)
        self.proposer = Status.NOT_DEFINED
        self.blockchain_file = f'blockchain_file_node_id_{self.node_id}'
        self.blockchain = Block.load_from_file(self.blockchain_file)
        self.view_number = 0 if len(self.blockchain) == 0 else len(self.blockchain) - 1
        self.state = State.PRE_PREPARE

        print(self.state)

        self.check_prepare = 0
        self.committed_messages_queue = []
        self.prepare_messages_queue = []
        self.preprepare_messges_queue = []
        self.check_committed = 0
        self.block = None
        self.new_round = False

    def ConnectToNode(self, request, context):
        if request.node_id not in self.connected_nodes:
            self.connected_nodes.append(request)
            if len(self.transactions) > 0 or len(self.blockchain) > 0:
                self.request_blockchain_and_transactions(request)
            else:
                self.request_blockchain_and_transactions_self(request)

            return p2p_pb2.ConnectionResponse(success=True, message="Connection successful")
        else:
            return p2p_pb2.ConnectionResponse(success=False, message="Node already connected")

    def request_blockchain_and_transactions(self, request):
        try:
            channel = grpc.insecure_channel(request.address)
            client = p2p_pb2_grpc.NodeStub(channel)
            client.ReceiveTransactionsConnect(self.get_transactions())
            client.ReceiveBlockchainConnect(self.get_blockchain())
        except grpc.RpcError as e:
            pass

    def request_blockchain_and_transactions_self(self, request):
        try:
            channel = grpc.insecure_channel(request.address)
            client = p2p_pb2_grpc.NodeStub(channel)
            client.BroadcastTransactionsConnect(p2p_pb2.NodeInfo(node_id=self.node_id, address=self.address))
            client.BroadcastBlockchainConnect(p2p_pb2.NodeInfo(node_id=self.node_id, address=self.address))
        except grpc.RpcError as e:
            pass

    def BroadcastTransactionsConnect(self, request, context):
        try:
            channel = grpc.insecure_channel(request.address)
            client = p2p_pb2_grpc.NodeStub(channel)
            client.ReceiveTransactionsConnect(self.get_transactions())
        except grpc.RpcError as e:
            pass
        return self.get_transactions()

    def ReceiveTransactionsConnect(self, request, context):
        if len(self.transactions) == 0:
            for transaction in request.transactions:
                transaction_add = Transaction(
                    transaction.node_id, transaction.data, transaction.timestamp
                )
                self.transactions.append(transaction_add)
                self.save_transactions(transaction_add)
        return p2p_pb2.Empty()

    def ReceiveBlockchainConnect(self, request, context):
        if len(self.blockchain) == 0:
            for block in request.blocks:
                block_add = Block(
                    index=str(block.index), timestamp=block.timestamp, previous_hash=str(block.previous_hash),
                    transactions=self.deserialize_transactions(block.transactions), hash=str(block.hash)
                )
                self.blockchain.append(block_add)
                self.view_number = len(self.blockchain) - 1
                self.save_block(block_add)
        return p2p_pb2.Empty()

    def BroadcastBlockchainConnect(self, request, context):
        try:
            channel = grpc.insecure_channel(request.address)
            client = p2p_pb2_grpc.NodeStub(channel)
            client.ReceiveBlockchainConnect(self.get_blockchain())
        except grpc.RpcError as e:
            pass
        return self.get_blockchain()


    def GetConnectedNodes(self, request, context):
        return p2p_pb2.NodeList(nodes=self.connected_nodes)

    def heartbeat_loop(self):
        while True:
            self.send_heartbeat()
            time.sleep(5)

    def send_heartbeat(self):
        connected_nodes_copy = self.connected_nodes[:]
        current_node = p2p_pb2.NodeInfo(node_id=self.node_id, address=self.address)
        for node in connected_nodes_copy:
            try:
                channel = grpc.insecure_channel(node.address)
                client = p2p_pb2_grpc.NodeStub(channel)
                client.BroadcastHeartbeat(p2p_pb2.HeartbeatMessage(nodes=self.connected_nodes, node_first=current_node))
            except grpc.RpcError:
                self.connected_nodes.remove(node)
                self.notify_nodes_about_removal(node)

    def BroadcastHeartbeat(self, request, context):
        received_nodes = request.nodes
        node_first = request.node_first
        self.merge_node_list(received_nodes, node_first)
        return p2p_pb2.Empty()

    def merge_node_list(self, received_nodes, node_first):
        exiting_node_ids = set(node.node_id for node in self.connected_nodes)
        for node in received_nodes:
            if node.node_id not in exiting_node_ids and node.node_id != self.node_id:

                print(f"Add node: node_id: {node.node_id}, address: {node.address}")

                self.connected_nodes.append(node)
        if node_first.node_id not in exiting_node_ids:

            print(f"Add node first: {node_first.node_id}, address: {node_first.address}")

            self.connected_nodes.append(node_first)

    def notify_nodes_about_removal(self, removed_node):
        for node in self.connected_nodes:
            try:
                channel = grpc.insecure_channel(node.address)
                client = p2p_pb2_grpc.NodeStub(channel)
                client.HandleNodeRemoval(removed_node)
            except grpc.RpcError:
                pass

    def HandleNodeRemoval(self, request, context):
        self.connected_nodes = [node for node in self.connected_nodes if node.node_id != request.node_id]
        return p2p_pb2.Empty()

    def BroadcastTransaction(self, request, context):
        transaction = Transaction(node_id=request.node_id, data=request.data, timestamp=request.timestamp)

        print(
            f"Broadcast transaction: (node_id: {request.node_id}, data: {request.data}, timestamp: {request.timestamp}"
        )

        self.transactions.append(transaction)
        self.save_transactions(transaction)

        print("Save transaction to file")

        for node in self.connected_nodes:
            try:
                channel = grpc.insecure_channel(node.address)
                client = p2p_pb2_grpc.NodeStub(channel)
                client.ReceiveTransactions(request)
            except grpc.RpcError as e:
                pass

        self.check_and_select_proposer()

        return p2p_pb2.Empty()

    def save_transactions(self, transaction):
        transaction.save_to_file(self.transactions_file)

    def ReceiveTransactions(self, request, context):
        transaction = Transaction(node_id=request.node_id, data=request.data, timestamp=request.timestamp)

        print(
            f"ReceiveTransactions: (node_id: {request.node_id}, data: {request.data}, timestamp: {request.timestamp})"
        )

        self.transactions.append(transaction)
        self.save_transactions(transaction)

        print("Save transaction to file")

        self.check_and_select_proposer()
        if len(self.preprepare_messges_queue) > 0:
            print("Handle preprepare_messges_queue")
        while self.preprepare_messges_queue:
            message = self.preprepare_messges_queue.pop(0)
            if self.state == State.PRE_PREPARE and len(self.blockchain) > 0 and len(self.transactions) >= 2:
                self.state = State.PREPARE
            if len(self.blockchain) > 0 and self.is_valid_block(self.block) and self.state == State.PREPARE:
                self.send_prepared()
                self.process_prepare_queue()
        self.process_prepare_queue()

        return p2p_pb2.Empty()

    def GetTransactions(self, request, context):
        grpc_transactions = []
        for transaction in self.transactions:

            if transaction.timestamp == "":
                formatted_timestamp = ""
            elif isinstance(transaction.timestamp, datetime):
                formatted_timestamp = transaction.timestamp.isoformat()
            else:
                timestamp = datetime.fromisoformat(transaction.timestamp)
                formatted_timestamp = timestamp.isoformat()

            grpc_transaction = p2p_pb2.Transaction(
                node_id=transaction.node_id, data=transaction.data, timestamp=formatted_timestamp
            )
            grpc_transactions.append(grpc_transaction)
        return p2p_pb2.TransactionsList(transactions=grpc_transactions)

    def get_transactions(self):
        grpc_transactions = []
        for transaction in self.transactions:

            if transaction.timestamp == "":
                formatted_timestamp = ""
            elif isinstance(transaction.timestamp, datetime):
                formatted_timestamp = transaction.timestamp.isoformat()
            else:
                timestamp = datetime.fromisoformat(transaction.timestamp)
                formatted_timestamp = timestamp.isoformat()

            grpc_transaction = p2p_pb2.Transaction(
                node_id=transaction.node_id, data=transaction.data, timestamp=formatted_timestamp
            )
            grpc_transactions.append(grpc_transaction)
        return p2p_pb2.TransactionsList(transactions=grpc_transactions)

    def get_blockchain(self):
        blockchain_list = []
        for block in self.blockchain:
            if block.timestamp == "":
                formatted_timestamp = ""
            elif isinstance(block.timestamp, datetime):
                formatted_timestamp = block.timestamp.isoformat()
            else:
                timestamp = datetime.fromisoformat(block.timestamp)
                formatted_timestamp = timestamp.isoformat()

            grpc_block = p2p_pb2.Block(
                index=str(block.index), timestamp=formatted_timestamp, previous_hash=str(block.previous_hash),
                transactions=self.serialize_transactions(block.transactions), hash=str(block.hash)
            )
            blockchain_list.append(grpc_block)
        return p2p_pb2.BlockchainList(blocks=blockchain_list)

    def select_proposer(self):
        node_id_proposer = self.view_number % (len(self.connected_nodes) + 1)
        node_ids = [int(node.node_id) for node in self.connected_nodes]

        if int(node_id_proposer) == int(self.node_id):
            self.proposer = Status.TRUE
            return

        while node_id_proposer not in node_ids:
            node_id_proposer += 1
            if int(self.node_id) == int(node_id_proposer):
                break
            if int(node_id_proposer) > max(node_ids):
                node_id_proposer = min(node_ids)

        if int(node_id_proposer) == int(self.node_id):
            self.proposer = Status.TRUE
        else:
            self.proposer = Status.FALSE

    def check_and_select_proposer(self):
        self.new_round = False
        if len(self.transactions) >= 2:
            if self.proposer == Status.NOT_DEFINED:
                self.select_proposer()

                print("Is proposer: ", self.proposer)

            if self.proposer == Status.TRUE:
                if len(self.blockchain) == 0:
                    new_genesis_block = self.create_block()
                    self.send_block(new_genesis_block)
                new_block = self.create_block()
                self.send_block(new_block)

    def create_block(self):
        if len(self.blockchain) == 0:
            new_block = Block(index=len(self.blockchain), transactions=self.transactions, previous_hash=0, timestamp="")
        else:
            new_block = Block(
                index=len(self.blockchain), transactions=self.transactions, previous_hash=self.blockchain[-1].hash
            )

        self.block = new_block

        print("Create block")

        return new_block

    def send_block(self, new_block):
        self.committed_messages_queue = []
        self.prepare_messages_queue = []
        if new_block is not None and not self.new_round:

            if len(self.blockchain) == 0:
                self.blockchain.append(new_block)
                self.save_block(new_block)

            if new_block.timestamp == "":
                formatted_timestamp = ""
            elif isinstance(new_block.timestamp, datetime):
                formatted_timestamp = new_block.timestamp.isoformat()
            else:
                timestamp = datetime.fromisoformat(new_block.timestamp)
                formatted_timestamp = timestamp.isoformat()

            new_block_grpc = p2p_pb2.Block(
                index=str(new_block.index), timestamp=formatted_timestamp, previous_hash=str(new_block.previous_hash),
                transactions=self.serialize_transactions(new_block.transactions)
            )

            print("Send block")

            if self.state == State.PRE_PREPARE and len(self.blockchain) > 0 and self.new_round is False:
                self.state = State.PREPARE

                print(self.state)

            for node in self.connected_nodes:
                try:
                    channel = grpc.insecure_channel(node.address)
                    client = p2p_pb2_grpc.NodeStub(channel)
                    client.ReceivePrePrepare(request=new_block_grpc)
                except grpc.RpcError as e:
                    pass

    def ReceivePrePrepare(self, request, context):
        transactions = self.deserialize_transactions(request.transactions)
        block = Block(
            index=request.index, timestamp=request.timestamp, transactions=transactions,
            previous_hash=request.previous_hash
        )

        print("Receive block")

        if len(self.blockchain) == 0:
            self.blockchain.append(block)
            self.save_block(block)
        else:
            self.block = block

        if self.state == State.PRE_PREPARE and len(self.blockchain) > 0 and len(self.transactions) >= 2:
            if self.proposer.NOT_DEFINED:
                self.check_and_select_proposer()
            self.state = State.PREPARE

            print(self.state)

        elif self.state == State.PRE_PREPARE and len(self.blockchain) > 0:
            self.preprepare_messges_queue.append(request)

        if len(self.blockchain) > 0 and self.is_valid_block(block) and self.state == State.PREPARE:
            self.send_prepared()
            self.process_prepare_queue()

        return p2p_pb2.Empty()

    def serialize_transactions(self, transactions):
        grpc_transactions = []
        for transaction in transactions:
            if isinstance(transaction, Transaction):
                if transaction.timestamp == "":
                    formatted_timestamp = ""
                elif isinstance(transaction.timestamp, datetime):
                    formatted_timestamp = transaction.timestamp.isoformat()
                else:
                    timestamp = datetime.fromisoformat(transaction.timestamp)
                    formatted_timestamp = timestamp.isoformat()

                grpc_transaction = p2p_pb2.Transaction(
                    node_id=transaction.node_id,
                    data=transaction.data,
                    timestamp=formatted_timestamp
                )
                grpc_transactions.append(grpc_transaction)
            elif isinstance(transaction, dict):
                transaction_timestamp = transaction.get("timestamp", "")
                if transaction_timestamp and isinstance(transaction_timestamp, str):
                    try:
                        formatted_timestamp = datetime.fromisoformat(transaction_timestamp).isoformat()
                    except ValueError:
                        formatted_timestamp = ""
                else:
                    formatted_timestamp = ""

                grpc_transaction = p2p_pb2.Transaction(
                    node_id=transaction.get("node_id", ""),
                    data=transaction.get("data", ""),
                    timestamp=formatted_timestamp
                )
                grpc_transactions.append(grpc_transaction)
            else:
                continue
        return grpc_transactions

    def deserialize_transactions(self, grpc_transactions):
        transactions = []
        for grpc_transaction in grpc_transactions:
            timestamp = datetime.fromisoformat(grpc_transaction.timestamp) if grpc_transaction.timestamp else None
            transaction = Transaction(
                node_id=grpc_transaction.node_id,
                data=grpc_transaction.data,
                timestamp=timestamp
            )
            transactions.append(transaction)
        return transactions

    def is_valid_block(self, block):
        last_block = self.blockchain[-1]
        if int(block.index) != int(last_block.index) + 1:
            return False
        if block.previous_hash != last_block.hash:
            return False
        if str(block.calculate_hash()) != str(block.hash):
            return False

        print("Block is valid")

        return True

    def send_prepared(self):
        print("Send prepared")

        for node in self.connected_nodes:
            try:
                channel = grpc.insecure_channel(node.address)
                client = p2p_pb2_grpc.NodeStub(channel)
                client.ReceivePrepare(p2p_pb2.Empty())
            except grpc.RpcError as e:
                pass
        if self.state == State.PREPARE:
            self.state = State.COMMITTED

            print(self.state)

    def ReceivePrepare(self, request, context):
        print("ReceivePrepare")

        if self.new_round:
            return p2p_pb2.Empty()

        if self.proposer == Status.TRUE and self.state == State.PREPARE:
            self.state = State.COMMITTED

            print(self.state)

        if ((self.proposer == Status.FALSE or self.proposer == Status.NOT_DEFINED) and self.state != State.COMMITTED
                and not self.new_round):

            print("Add ReceivePrepare to queue")

            self.prepare_messages_queue.append(request)
        else:
            self.handle_to_prepared_message(request)
        return p2p_pb2.Empty()

    def handle_to_prepared_message(self, message):
        print("Handle prepare")

        if not self.new_round:
            self.check_prepare += 1
            if self.check_prepare > self.max_faulty_nodes(len(self.connected_nodes) + 1):

                print("Check prepare: ")

                self.send_committed()
                self.process_committed_queue()
            else:
                pass

    def process_prepare_queue(self):
        while self.prepare_messages_queue:
            message = self.prepare_messages_queue.pop(0)
            self.handle_to_prepared_message(message)

    def max_faulty_nodes(self, N):
        return (N - 1) // 3

    def send_committed(self):
        print("Send committed")

        if self.state == State.COMMITTED:
            for node in self.connected_nodes:
                try:
                    channel = grpc.insecure_channel(node.address)
                    client = p2p_pb2_grpc.NodeStub(channel)
                    client.ReceiveCommitted(p2p_pb2.Empty())
                except grpc.RpcError as e:
                    pass
            if self.state == State.COMMITTED:
                self.state = State.INSERTED

                print(self.state)

        else:
            pass

    def ReceiveCommitted(self, request, context):
        print("ReceiveCommitted")

        if not self.new_round:
            if self.state != State.INSERTED and not self.new_round:
                self.committed_messages_queue.append(request)

                print("Add ReceiveCommitted to queue")

            else:
                self.handle_committed_message(request)
            return p2p_pb2.Empty()

    def handle_committed_message(self, message):
        print("Handle committed")
        self.check_committed += 1
        if self.check_committed > self.max_faulty_nodes(len(self.connected_nodes) + 1):

            print("Check committed")

            self.inserted()
        else:
            pass

    def process_committed_queue(self):
        while self.committed_messages_queue:
            message = self.committed_messages_queue.pop(0)
            self.handle_committed_message(message)

    def inserted(self):
        self.blockchain.append(self.block)

        print("Add block to blockchain")

        self.save_block(self.block)

        print("Save block to file")

        self.save_round()
        self.state = State.PRE_PREPARE

        print(self.state)

        self.view_number += 1

        print("View round: ", self.view_number)

        self.transactions = []
        self.check_committed = 0
        self.check_prepare = 0
        self.committed_messages_queue = []
        self.prepare_messages_queue = []
        self.preprepare_messges_queue = []
        self.proposer = Status.NOT_DEFINED
        self.block = []
        self.new_round = True

    def GetBlockchain(self, request, context):
        blockchain_list = []
        for block in self.blockchain:
            if block.timestamp == "":
                formatted_timestamp = ""
            elif isinstance(block.timestamp, datetime):
                formatted_timestamp = block.timestamp.isoformat()
            else:
                timestamp = datetime.fromisoformat(block.timestamp)
                formatted_timestamp = timestamp.isoformat()

            grpc_block = p2p_pb2.Block(
                index=str(block.index), timestamp=formatted_timestamp, previous_hash=str(block.previous_hash),
                transactions=self.serialize_transactions(block.transactions), hash=str(block.hash)
            )

            blockchain_list.append(grpc_block)
        return p2p_pb2.BlockchainList(blocks=blockchain_list)

    def save_round(self):
        with open(self.transactions_file, 'a') as file:
            file.write("---END OF ROUND---")
            file.write('\n')

    def save_block(self, block):
        block.save_to_file(self.blockchain_file)


def serve():
    port = input("Enter address: ")
    node_id = input("Enter node_id: ")
    address = f'{port}'
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    node_service = Node(node_id, address)
    p2p_pb2_grpc.add_NodeServicer_to_server(node_service, server)
    server.add_insecure_port(f'{address}')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    serve()
