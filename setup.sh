ENV_PATH='/home/'$USER'/envs/'
ENV_NAME='lacrosse_prediction'
ENV=$ENV_PATH$ENV_NAME'/'
ENV_ACT="/bin/activate"

if [ ! -d $ENV ]; then
    mkdir -p $ENV;
    echo "directory "$ENV" created "
fi

if [ ! -d $ENV'/bin/' ]; then
    echo "creating virutal environment in dir "$ENV
    python3 -m venv $ENV
    echo "scuccess"
    echo "activate env"
    source $ENV$ENV_ACT
    echo "success"
    echo "upgrade pip"
    python -m pip install --upgrade pip
    echo "success"
    echo "installing dependecies using requirements.txt"
    pip install -r requirements.txt
    echo "success"
fi