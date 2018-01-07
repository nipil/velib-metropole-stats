# velib-metropole-stats

Statistics about the new "velib-metropole" bike-sharing service in Paris, France

# install

    sudo apt-get install \
      --no-install-recommends \
      --no-install-suggests \
      git gcc python3-venv python3-dev

    git clone https://github.com/nipil/velib-metropole-stats.git
    cd velib-metropole-stats

    python3 -m venv venv
    venv/bin/pip3 install wheel
    venv/bin/pip3 install -r requirements.txt
