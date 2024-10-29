import clickhouse_connect

if __name__ == '__main__':
    client = clickhouse_connect.get_client(
        host='k9qhqz4fpf.us-east-1.aws.clickhouse.cloud',
        user='default',
        password='MklO7_H9DG_Z7',
        secure=True
    )
    print("Result:", client.query("SELECT 1").result_set[0][0])