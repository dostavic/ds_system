import datetime
import json


class Transaction:
    def __init__(self, node_id, data, timestamp=None):
        self.node_id = node_id
        self.timestamp = datetime.datetime.now() if timestamp is None else timestamp
        self.data = data

    def to_dict(self):
        return {
            "node_id": self.node_id,
            "timestamp": self.timestamp.isoformat() if isinstance(self.timestamp, datetime.datetime)
            else self.timestamp,
            "data": self.data
        }

    def save_to_file(self, filename):
        with open(filename, 'a') as file:
            json.dump(self.to_dict(), file)
            file.write('\n')

    @staticmethod
    def load_from_file(filename):
        with open(filename, 'r') as file:
            lines = file.readlines()

        last_round_index = None
        for i, line in enumerate(reversed(lines)):
            if line.strip() == "---END OF ROUND---":
                last_round_index = len(lines) - i
                break

        transactions = []
        for line in lines[last_round_index:]:
            if line.strip() != "---END OF ROUND---":
                transactions_data = json.loads(line)
                transaction = Transaction(
                    transactions_data['node_id'],
                    transactions_data['data'],
                    transactions_data['timestamp']
                )
                transaction.timestamp = datetime.datetime.fromisoformat(transactions_data['timestamp'])
                transactions.append(transaction)

        return transactions

