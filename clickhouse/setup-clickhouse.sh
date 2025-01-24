# install
sudo apt-get install -y apt-transport-https ca-certificates dirmngr
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 8919F6BD2B48D754

echo "deb https://packages.clickhouse.com/deb stable main" | sudo tee /etc/apt/sources.list.d/clickhouse.list
sudo apt-get update

sudo apt-get install -y clickhouse-server clickhouse-client

# stop server if service was already running
sudo service clickhouse-server start ||:


# modify clickhouse settings so data is stored on the mount.
sudo mkdir /var/lib/mount/clickhouse-nvme-mount/
sudo chown clickhouse:clickhouse /var/lib/mount/clickhouse-nvme-mount

# copy clickhouse config
sudo cp -a /var/lib/clickhouse/. /var/lib/mount/clickhouse-nvme-mount/
sudo cp clickhouse/clickhouse-mount-config.xml /etc/clickhouse-server/config.d/data-paths.xml


# start server
sudo rm /var/log/clickhouse-server/clickhouse-server.err.log /var/log/clickhouse-server/clickhouse-server.log
sudo service clickhouse-server start


MEMORY_LIMIT=0
BYTES_BEFORE_EXTERNAL_GROUP_BY=0
if [ $MACHINE_TYPE == "c6id.4xlarge" ]; then
	MEMORY_LIMIT=28000000000
	BYTES_BEFORE_EXTERNAL_GROUP_BY=20000000000
fi

clickhouse-client --query "CREATE USER IF NOT EXISTS db_benchmark IDENTIFIED WITH no_password SETTINGS max_memory_usage = $MEMORY_LIMIT, max_bytes_before_external_group_by = $BYTES_BEFORE_EXTERNAL_GROUP_BY WRITABLE;"
clickhouse-client --query "GRANT select, insert, create, alter, alter user,  create table, drop on *.* to db_benchmark;"
