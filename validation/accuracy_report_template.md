# WOfL accuracy report

Accuracy of Digital Earth Pacific WOfL wet/dry classes against hand-labelled
points. See the [validation README](../README.md) for how the points were
sampled and labelled. Generated {date} by `accuracy_report.py`.

## Points labelled

{point_count} points in {scene_count} scenes, by stratum (rows) and hand label
(columns). The {unknown_count} points labelled unknown are left out of
everything below.

{labelled_table}

## Overall accuracy

| | Accuracy (%) |
|:--|--:|
| All points | {overall_accuracy} |
| Inland points (GADM land) | {inland_accuracy} |

{cloud_percent}% of points were labelled cloud. These count as errors, since
WOfL classified them as wet or dry.

## Confusion matrices

WOfL class (rows) against hand label (columns), for all points:

{confusion_table}

And for inland points only:

{inland_confusion_table}

## User's and producer's accuracy (%)

{class_accuracy_table}

## Accuracy by stratum

Percent of each stratum's points given each hand label, and the stratum's
user's accuracy.

{stratum_table}

## Accuracy against number of scenes labelled

Points show each scene's accuracy and the line shows cumulative accuracy, with
scenes in the order they were labelled. A flat line suggests enough scenes
were labelled.

![Accuracy against number of scenes labelled](accuracy_by_scene.png)
