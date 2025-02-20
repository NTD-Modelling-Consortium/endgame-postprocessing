FROM python:3.12.0-alpine

RUN mkdir -p /ntdmc/
WORKDIR /ntdmc

RUN apk update && apk add git poetry
RUN git clone https://github.com/NTD-Modelling-Consortium/endgame-postprocessing.git
WORKDIR /ntdmc/endgame-postprocessing
RUN poetry install

ENTRYPOINT [ "poetry", "run", "python", "misc/pp_mixed_scenarios/post_process_mixed_scenarios.py" ]
