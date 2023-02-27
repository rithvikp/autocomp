#!/bin/bash

# The first argument should be the static internal IP of the NFS server

sudo apt-get update && sudo apt-get upgrade -y

curl https://raw.githubusercontent.com/mwhittaker/vms/master/install_java8.sh -sSf | bash

# Prepend to bashrc
sed -i '1s;^;source ~/.bash_path\n;' $HOME/.bashrc

source ~/.bashrc

mkdir -p "$HOME/install"
cd "$HOME/install"

wget -O cs.gz https://github.com/coursier/launchers/raw/master/cs-x86_64-pc-linux-static.gz
gzip -d cs
chmod +x cs

./cs setup -y

# Mount the shared dir
sudo apt-get install nfs-common -y
sudo mkdir /mnt/nfs

sudo mount -t nfs -o vers=4 -o resvport "$1:/share" /mnt/nfs

# Setup the mount on boot
echo -e '#!/bin/bash\nsudo mount -t nfs -o vers=4 -o resvport $1:/share /mnt/nfs' > mount.sh
chmod +x mount.sh

sudo bash -c 'echo -e "[Unit]\nDescription=Mount the shared FS\n\n[Service]\nExecStart=/home/rithvik/install/mount.sh\nType=oneshot\nRemainAfterExit=yes\n\n[Install]\nWantedBy=multi-user.target" > /etc/systemd/system/mount.service'

sudo systemctl daemon-reload
sudo systemctl enable mount.service
