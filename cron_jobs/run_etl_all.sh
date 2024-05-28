source /home/$USER/envs/lacrosse_prediction/bin/activate
PARENT_DIR="$(dirname "$(dirname "$(realpath "$0")")")"
cd $PARENT_DIR
ETL_DIR=$PARENT_DIR/etl
echo "running commitments etl"
python  $ETL_DIR/etl_commitments.py
echo "running camps and clinics etl"
python  $ETL_DIR/etl_camps_and_clinics.py
echo "running clubs ranking etl"
python $ETL_DIR/etl_clubs_ranking.py
echo "running colleges ranking etl"
python $ETL_DIR/etl_colleges_ranking.py
echo "running schools ranking etl"
python $ETL_DIR/etl_schools_ranking.py

