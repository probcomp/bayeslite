FROM        ubuntu

MAINTAINER  Baxter Eaves

RUN         apt-get update
RUN         apt-get install -y git dialog wget nano python2.7-dev python-pip libboost1.54-all-dev libatlas-dev libblas-dev liblapack-dev apt-utils ccache gfortran

# pip install various python libraries
RUN         pip install -U distribute && pip install cython && pip install numpy && pip install scipy && pip install patsy && pip install pandas && pip install statsmodels && pip install pytest && pip install cmd2 && pip install pexpect

# Set up ssh key and clone git. I don't know how much of this I actuall need. Probably not all of it.
RUN             apt-get install -y openssh-client
RUN         mkdir /root/.ssh/
ADD         probcomp-gh-bot-id_rsa /root/.ssh/id_rsa
RUN             chmod 600 /root/.ssh/id_rsa
RUN             touch /root/.ssh/known_hosts
RUN         ssh-keyscan -v -t rsa github.com >> /root/.ssh/known_hosts

WORKDIR     /home/bayeslite
RUN         cd /home/bayeslite && git clone git@github.com:probcomp/crosscat.git

# git clone bayeslite
RUN         cd /home/bayeslite && git clone git@github.com:probcomp/bayeslite.git

# install crosscat and bayeslite
RUN         cd /home/bayeslite/crosscat && python setup.py develop
RUN         cd /home/bayeslite/bayeslite && python setup.py install

ENV         MYPASSWORD bayeslite
ENV         USER bayeslite

# make a nice readme
RUN         echo "\n\nroot password is $MYPASSWORD" >> readme.txt

# show readme at login
RUN         echo "cat ~/readme.txt" >> .bashrc && echo "export PYTHONPATH=/home/bayeslite/crosscat" >> .bashrc

# create a root and bayeslite password
RUN         echo "root:$MYPASSWORD" | chpasswd
RUN         useradd bayeslite
RUN         echo "bayeslite:$MYPASSWORD" | chpasswd
ENV         HOME /home/bayeslite

CMD         /bin/bash
