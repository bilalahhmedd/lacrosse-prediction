ENV_PATH='/home/'$USER'/envs/'
ENV_NAME='lacrosse_prediction_testing'
ENV=$ENV_PATH$ENV_NAME
if [ ! -d $ENV ]; then
    mkdir -p $ENV;
    echo "directory "$ENV" created "
fi
ENV_ACT="/bin/activate"
echo "creating virutal environment in dir "$ENV
python3 -m venv $ENV
echo "scuccess"
echo "activate env"
source $ENV$ENV_ACT
echo "success"
echo "installing dependecies using requirements.txt"
pip install -r requirements.txt
echo "success"
