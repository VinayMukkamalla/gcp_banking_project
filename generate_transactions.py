import csv
import random
import os
from datetime import datetime, timedelta

def generate_mock_banking_data(num_records=1000):
    # Setup baseline entities
    locations = ['Hyderabad', 'Mumbai', 'Bangalore', 'Delhi', 'Chennai']
    tx_types = ['CREDIT', 'DEBIT', 'TRANSFER']
    
    filename = "daily_transactions.csv"
    
    print(f"Generating {num_records} banking transactions...")
    
    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        # Define columns exactly matching enterprise banking schemas
        writer.writerow(['transaction_id', 'account_id', 'amount', 'transaction_type', 'timestamp', 'location'])
        
        start_time = datetime.now() - timedelta(days=1)
        
        for i in range(1, num_records + 1):
            tx_id = f"TXN{2026}{i:05d}" # Generates format: TXN202600001
            acc_id = f"ACC{random.randint(1000, 1050)}" # Reuses accounts to simulate activity
            amount = round(random.uniform(10.0, 50000.0), 2)
            tx_type = random.choice(tx_types)
            
            # Increment timestamps slightly to spread data across the day
            tx_time = (start_time + timedelta(seconds=i * 30)).strftime('%Y-%m-%d %H:%M:%S')
            loc = random.choice(locations)
            
            writer.writerow([tx_id, acc_id, amount, tx_type, tx_time, loc])
            
    print(f"Success! Saved sample raw data to {os.path.abspath(filename)}")

if __name__ == "__main__":
    generate_mock_banking_data(5000) # Generates 5,000 baseline rows
