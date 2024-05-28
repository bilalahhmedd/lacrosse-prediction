source /home/$USER/envs/lacrosse_prediction/bin/activate
PARENT_DIR="$(dirname "$(dirname "$(realpath "$0")")")"
cd $PARENT_DIR
SCRAPPER_DIR=$PARENT_DIR/scrappers
echo $SCRAPPER_DIR
echo "running commitments laxnumbers scrapper"
python  $SCRAPPER_DIR/commitments_laxnumbers_scrapper.py
echo "running commitments toplax recruits scrapper"
python  $SCRAPPER_DIR/commitments_toplaxrecruits_scrapper.py
echo "running commitments clublacrosse scrapper"
python  $SCRAPPER_DIR/commitments_clublacrosse_scrapper.py
echo "running commitments insidelacrosse scrapper"
python  $SCRAPPER_DIR/commitments_insidelacrosse_scrapper.py
echo "running commitments americanlacrosse scrapper"
python  $SCRAPPER_DIR/commitments_americanselectlacrosse_scrapper.py

