# endgame-postprocessing

## Tests

To run the tests for this repo run:

```
poetry run pytest
```

These are automatically run in CI.

### End to end tests

There exists end to end tests for the model post processing pipelines (currently just LF). 

They use a fixed input to generate a consistent set of outputs. 

If you are expecting the change, you can update them by running

```
poetry run pytest --snapshot-update
```

This will modify the files in the `known_good_output` directory. 
Verify the new data looks to have changed in the way you are expecting, 
and if it does then check it in. 
