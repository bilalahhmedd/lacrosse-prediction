source /home/$USER/envs/lacrosse_prediction/bin/activate
PARENT_DIR="$(dirname "$(dirname "$(realpath "$0")")")"
cd $PARENT_DIR
SCRAPPER_DIR=$PARENT_DIR/scrappers
echo "running schools ranking laxmath scrapper"
python  $SCRAPPER_DIR/schools_ranking_laxmath_scrapper.py
