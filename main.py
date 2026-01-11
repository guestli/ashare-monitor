import time
import datetime
import requests
import yaml
import pytz
import logging
import sys
import os
import akshare as ak
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

class StockMonitor:
    def __init__(self, config_path='config.yaml'):
        self.config = self.load_config(config_path)
        self.timezone = pytz.timezone('Asia/Shanghai')
        # Headers not strictly needed for akshare but kept if needed for other things
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
        }

    def load_config(self, path):
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            sys.exit(1)

    def is_trading_time(self):
        now = datetime.datetime.now(self.timezone)
        # Check if it's weekend
        if now.weekday() >= 5: # 5=Saturday, 6=Sunday
            return True

        current_time = now.time()
        
        for period in self.config.get('trading_hours', []):
            start_str = period['start']
            end_str = period['end']
            start_time = datetime.datetime.strptime(start_str, "%H:%M").time()
            end_time = datetime.datetime.strptime(end_str, "%H:%M").time()
            
            if start_time <= current_time <= end_time:
                return True
        
        return True

    def get_stock_data(self):
        stocks = self.config.get('stocks', [])
        if not stocks:
            return None

        results = []
        indices = []
        individual_stocks = []

        # Heuristic to separate indices from stocks
        for s in stocks:
            code = s['code']
            # SH Index: sh000xxx, SZ Index: sz399xxx
            if code.startswith('sh000') or code.startswith('sz399'):
                indices.append(s)
            else:
                individual_stocks.append(s)

        # 1. Fetch Bulk Index Data
        df_index = pd.DataFrame()
        try:
            # Add retry for bulk index fetch
            for _ in range(3):
                try:
                    df_index = ak.stock_zh_index_spot_em()
                    if not df_index.empty:
                        break
                except Exception:
                    time.sleep(1)
        except Exception as e:
            logger.error(f"Error fetching indices: {e}")

        # 2. Process Indices
        for s in indices:
            code_full = s['code']
            code_stripped = code_full[2:]
            
            found = False
            if not df_index.empty:
                # Attempt to find in bulk data
                # Need to match strict logic to avoid 000001 stock vs index confusion
                # df_index usually contains only indices, so matching code is safe(er)
                # But '000001' in df_index is SH Index.
                row = df_index[df_index['代码'] == code_stripped]
                if not row.empty:
                    try:
                        current_price = float(row.iloc[0]['最新价'])
                        change = float(row.iloc[0]['涨跌额'])
                        pct_change = float(row.iloc[0]['涨跌幅'])
                        
                        results.append({
                            'code': code_full,
                            'name': s.get('name', row.iloc[0]['名称']),
                            'price': current_price,
                            'change': change,
                            'pct_change': pct_change
                        })
                        found = True
                    except Exception as e:
                        logger.error(f"Error parsing index {code_full}: {e}")

            # If not found in bulk (e.g. 399001 often missing in EM bulk spot), try single fetch
            # But ONLY if it's likely to work (399xxx works, 000xxx fails/ambiguous)
            if not found:
                if code_stripped.startswith('399'):
                    # 399xxx works with stock_bid_ask_em
                    res = self.fetch_single_stock_with_retry(s)
                    if res:
                        results.append(res)
                else:
                    logger.warning(f"Index {code_full} not found in bulk data and skipped fallback.")

        # 3. Process Stocks
        if individual_stocks:
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = {executor.submit(self.fetch_single_stock_with_retry, s): s for s in individual_stocks}
                for future in futures:
                    res = future.result()
                    if res:
                        results.append(res)
        
        return results

    def fetch_single_stock_with_retry(self, stock_config, retries=3):
        for i in range(retries):
            res = self.fetch_single_stock(stock_config)
            if res:
                return res
            time.sleep(0.5)
        return None

    def fetch_single_stock(self, stock_config):
        code_full = stock_config['code']
        code_stripped = code_full[2:] if code_full.startswith(('sh', 'sz')) else code_full
        
        try:
            # Use ak.stock_bid_ask_em for real-time quote
            df = ak.stock_bid_ask_em(symbol=code_stripped)
            
            price_row = df[df['item'] == '最新']
            change_row = df[df['item'] == '涨跌']
            pct_row = df[df['item'] == '涨幅']
            
            if price_row.empty:
                return None
            
            # Helper to safely parse float/str
            def parse_val(val):
                if isinstance(val, (float, int)):
                    return float(val)
                return float(str(val).replace('-', '0')) # Handle '-'
                
            current_price = parse_val(price_row.iloc[0]['value'])
            change = parse_val(change_row.iloc[0]['value'])
            pct_change = parse_val(pct_row.iloc[0]['value'])
            
            return {
                'code': code_full,
                'name': stock_config.get('name', code_full),
                'price': current_price,
                'change': change,
                'pct_change': pct_change
            }
        except Exception as e:
            # Don't log on every retry fail, maybe just debug or warning if all fail
            # logger.debug(f"Fetch failed for {code_full}: {e}")
            return None


    def format_message(self, data):
        if not data:
            return None
            
        # lines = [f"📊 A股行情推送 {datetime.datetime.now(self.timezone).strftime('%H:%M:%S')}"]
        lines = [f" {datetime.datetime.now(self.timezone).strftime('%H:%M:%S')}"]
        # lines.append("-" * 20)
        lines.append("\n")

        for item in data:
            # emoji = "🔴" if item['change'] > 0 else "🟢" if item['change'] < 0 else "⚪"
            emoji = ""
            sign = "+" if item['change'] > 0 else ""
            # line = f"{emoji} {item['name']}: {item['price']:.2f} ({sign}{item['change']:.2f}, {sign}{item['pct_change']:.2f}%)"
            line = f"{emoji} {item['name']}: {item['price']:.2f} ({sign}{item['pct_change']:.2f}%)"
            lines.append(line)
            
        return "\n".join(lines)

    def send_webhook(self, message):
        url = os.environ.get('WEBHOOK_URL') or self.config.get('webhook_url')
        if not url:
            logger.warning("No webhook URL configured.")
            print(message) # Print to console if no webhook
            return

        # Simple heuristic to determine payload format
        payload = {}
        if "dingtalk" in url:
            payload = {
                "msgtype": "text",
                "text": {
                    "content": message
                }
            }
        elif "feishu" in url:
            payload = {
                "msg_type": "text",
                "content": {
                    "text": message
                }
            }
        else:
            # Generic
            payload = {"text": message, "content": message}

        try:
            resp = requests.post(url, json=payload, timeout=10)
            if resp.status_code == 200:
                logger.info("Notification sent successfully.")
            else:
                logger.error(f"Failed to send notification: {resp.text}")
        except Exception as e:
            logger.error(f"Error sending webhook: {e}")

    def run(self):
        logger.info("Starting A-Share Monitor...")
        interval = self.config.get('interval', 60)
        
        while True:
            if self.is_trading_time():
                logger.info("Fetching stock data...")
                data = self.get_stock_data()
                if data:
                    msg = self.format_message(data)
                    self.send_webhook(msg)
                else:
                    logger.warning("No data fetched.")
            else:
                logger.info("Market closed. Waiting...")
            
            time.sleep(interval)

if __name__ == "__main__":
    monitor = StockMonitor()
    monitor.run()
