from cassandra.cluster import Cluster
import sys, re, os, gzip, uuid
from datetime import datetime
from cassandra.query import BatchStatement, SimpleStatement

def main(input_dir, keyspace, table_name):
    cluster = Cluster(['199.60.17.103', '199.60.17.105'])
    session = cluster.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': 2}" % keyspace)
    session.set_keyspace(keyspace)
    session.execute("DROP TABLE IF EXISTS " + keyspace + "." + table_name)
    session.execute("CREATE TABLE IF NOT EXISTS table_name (id UUID, host TEXT, datetime TIMESTAMP, path TEXT, bytes INT, PRIMARY KEY (host, id))")
    # session.execute('TRUNCATE table_name')

    batch = BatchStatement()
    count = 0
    for f in os.listdir(input_dir):
        with gzip.open(os.path.join(input_dir, f), 'rt', encoding='utf-8') as logfile:
            for line in logfile:
                line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
                splitted = line_re.split(line)
                if len(splitted) == 6:
                    batch.add(SimpleStatement("INSERT INTO table_name (id, host, datetime, path, bytes) VALUES (%s, %s, %s, %s, %s)"), (uuid.uuid4(), splitted[1], datetime.strptime(splitted[2],'%d/%b/%Y:%H:%M:%S').strftime('%Y-%m-%d %H:%M:%S'), splitted[3], int(splitted[4])))
                    count +=1
                    if count == 300:
                        session.execute(batch)
                        count = 0
                        batch.clear()

    session.execute(batch)
    batch.clear()
    table = session.execute("SELECT path, bytes FROM table_name WHERE host='uplherc.upl.com'")
    cont = 1
    total_bytes = 0
    for row in table:
        if row.bytes:
            cont += 1
            total_bytes += row.bytes
    print("No. of times uplherc.upl.com occurred: {}".format(cont))
    print("No. of bytes: {}".format(total_bytes))
    session.shutdown()


if __name__=="__main__":
    input_dir = sys.argv[1]
    keyspace = sys.argv[2]
    table_name = sys.argv[3]
    main(input_dir, keyspace, table_name)