#/bin/bash
source .env

cd task
bash compiletask.sh
cd ..

cd dist
bash compilesrvr.sh
cd ..

cd clnt
bash compileclnt.sh
cd ..
