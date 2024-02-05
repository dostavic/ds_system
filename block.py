import datetime
import hashlib
import json
import os


class Block:
    def __init__(self, index, transactions, previous_hash, timestamp=None, hash=None):
        self.index = index
        if timestamp == "0":
            self.timestamp = ""
        elif timestamp:
            self.timestamp = datetime.datetime.fromisoformat(timestamp) if isinstance(timestamp, str) else timestamp
        elif timestamp == "":
            self.timestamp = ""
        else:
            self.timestamp = datetime.datetime.now()

        self.transactions = transactions if previous_hash != 0 else []
        self.previous_hash = previous_hash
        if hash is None:
            self.hash = self.calculate_hash()
        else:
            self.hash = hash

    def calculate_hash(self):
        if self.timestamp != "":
            timestamp = self.timestamp.isoformat()
        else:
            timestamp = "No timestamp available"

        if self.transactions:
            transaction_data = [tx.to_dict() for tx in self.transactions]
        else:
            transaction_data = []

        transaction_json = json.dumps(transaction_data)
        block_data = f'{self.index}{timestamp}{transaction_json}{self.previous_hash}'
        encoded_block_data = block_data.encode()
        return hashlib.sha256(encoded_block_data).hexdigest()

    def to_dict(self):
        return {
            'index': self.index,
            'timestamp': self.timestamp.isoformat() if isinstance(self.timestamp, datetime.datetime) else "0",
            'transactions': [tx.to_dict() for tx in self.transactions],
            'previous_hash': self.previous_hash,
            'hash': self.hash
        }

    def save_to_file(self, filename):
        with open(filename, 'a') as file:
            json.dump(self.to_dict(), file)
            file.write('\n')

    @staticmethod
    def load_from_file(filename):
        if not os.path.exists(filename):
            return []

        blockchain = []
        with open(filename, 'r') as file:
            for line in file:
                block_data = json.loads(line)
                block = Block(
                    index=block_data['index'],
                    timestamp=block_data['timestamp'],
                    transactions=block_data['transactions'],
                    previous_hash=block_data['previous_hash'],
                    hash=block_data['hash']
                )
                blockchain.append(block)
        return blockchain
