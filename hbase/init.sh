echo 'Starting HBase...'
start-hbase.sh &
hbase thrift start -p 9090 &

echo 'Waiting for HBase to be up...'
while ! echo 'status' | hbase shell &>/dev/null; do sleep 5; done

echo 'Creating tables...'
if [ ! -f table.blocks ]; then
  echo "create 'blocks', 'x'" | hbase shell
  touch table.blocks
fi
if [ ! -f table.entries ]; then
  echo "create 'entries', 'x'" | hbase shell
  touch table.entries
fi
if [ ! -f table.tx ]; then
  echo "create 'tx', 'x'" | hbase shell
  touch table.tx
fi
if [ ! -f table.tx-by-addr ]; then
  echo "create 'tx-by-addr', 'x'" | hbase shell
  touch table.tx-by-addr
fi
if [ ! -f table.tx_full ]; then
  echo "create 'tx-by-block', 'x'" | hbase shell
  touch table.tx-by-block
fi
echo 'Tables created successfully'

wait
