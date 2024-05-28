source /home/$USER/envs/lacrosse_prediction/bin/activate
PARENT_DIR="$(dirname "$(dirname "$(realpath "$0")")")"
cd $PARENT_DIR
SCRAPPER_DIR=$PARENT_DIR/scrappers
echo "running clubs ranking insidelacrosse scrapper"
python  $SCRAPPER_DIR/clubs_ranking_insidelacrosse_scrapper.py
