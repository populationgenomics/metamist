# Tangled Tree component overview and mechanics

This component tries to draw a single pedigree for a given list of individuals and their relationships.

The input is a single pedigree file with columns for family id, individual id, paternal id, maternal id, sex and affected status.

This input file needs to be arranged into a more structured data type to pass to the rendering function. This occurs in the formatData() function.

## formatData() function

This function takes in an unordered pedigree file and returns an array of levels, which map to levels of a drawn pedigree (i.e. individuals on the same horizontal axis such as sets of siblings, parents, cousins etc are on the same level). Each level is an array of entries containing an individual id and an array of parents if they exist. The entries contain some additional information that need to be initialised but this is the gist of it.

### Example:

TRIO

input:

```json
[
    {
        "family_id": "TRIO",
        "individual_id": "Parent_1",
        "paternal_id": null,
        "maternal_id": null,
        "sex": 1,
        "affected": 2
    },
    {
        "family_id": "TRIO",
        "individual_id": "Parent_2",
        "paternal_id": null,
        "maternal_id": null,
        "sex": 2,
        "affected": 2
    },
    {
        "family_id": "TRIO",
        "individual_id": "Trio_Child",
        "paternal_id": "Parent_1",
        "maternal_id": "Parent_2",
        "sex": 1,
        "affected": 1
    }
]
```

Output:

```[
    {
        "nodes": [
            {
                "x": 0,
                "y": 0,
                "parents": [],
                "level": -1,
                "bundles": [],
                "links": [],
                "parentsList": [],
                "id": "Parent_1"
            },
            {
                "x": 0,
                "y": 0,
                "parents": [],
                "level": -1,
                "bundles": [],
                "links": [],
                "parentsList": [],
                "id": "Parent_2"
            }
        ],
        "bundles": []
    },
    {
        "nodes": [
            {
                "x": 0,
                "y": 0,
                "parents": [],
                "level": -1,
                "bundles": [],
                "links": [],
                "parentsList": [
                    "Parent_1",
                    "Parent_2"
                ],
                "id": "Trio_Child"
            }
        ],
        "bundles": []
    }
]
```

Note that the red is the 2 parent nodes, both in the same object (level 0) and the blue is the child node with a parentList array, this is level 1.

Generating this output correctly can be trivial for easy families (trios, standard nuclear, 3 generations etc) but can be tricky when for more complicated cases. It is likely that some tweaking will be necessary when we come across these cases. I've outlined the logic below. The idea is to identify the longest possible spine of consecutive descendants across generations, and then add branches where possible/necessary.

### Logic:

Identify all possible roots of the pedigree. These are individuals with no parents (ie no parental and maternal ids).

Find the root(s) with the largest amount of generations of descendants. This is done by recursively checking if an individual has children. The longest root is chosen to build our pedigree on. In the event of multiple roots with the same longest length, the first is arbitrarily chosen.

All other individuals other than the root are added to a Set of unseen from which they will be removed as they are encountered.

Using a queue and starting with the root we chose, we pop the next individual or group of individuals off the list and add them all on the same level. Then we add all their children to the queue, these will go on the next level. Note this step does not include spouses, just simply starting with the root on level 0, all their children on level 1, childrens children on level 2 etc.

After this 'spine' is completed, we try to add any remaining individuals to the pedigree in the appropriate level. First we check if the individual is a spouse of someone already added. If so, we add them to the same level.

Next, we check if the individual is a parents of someone already added. They will be added in one level higher than the individual (-1).

Finally if the individual is a child of someone already added, we add them at the level below (+1).

We repeat through all individuals, resetting a counter each time someone is able to be added. When a pass through all unadded individuals completes without any further additions, the function is complete.

Next, this formatted level data is passed to the constructTangleLayout() function which calculates 2d coordinates for each individual as well as the shapes of the branches connecting individuals.

## constructTangleLayout() function

This is likely the function that needs the most refactoring as it relies on deepcloning objects and recursive properties to work, which I don't think are necessary but haven't sat down to refactor it. This is inspired by https://observablehq.com/@nitaku/tangled-tree-visualization-ii

### Logic:

Basically we give coordinates to each individual of each level, incrementing by some spacing variable across each level. Eg parents in level 0 would be at (0, 0) and (0, 50). Then individuals at level 1 might be at (100, 0), (100, 50), (100, 100) and (100, 150) etc.

Now we try and move parents around on their levels so that they are placed together and not further left than their leftmost child. This is done in reverse through the levels so a parents is moved to be above their children, then the grandparents will be moved to be above the parents etc etc.

Next we centre the parents above all the children and move around an individuals without children who tend to get in the way.

Finally once the individuals are in their places, we create the series of lines between parents connecting to children.

This is all then passed to a render function which simply loops over the list of connections and draws them, then loops over all individuals and draws a circle or square at the given coordinate. Fill is given for affected status. Here is where we can add other features like twins etc just by looking up some data if we want.

Was thinking this might be a fun project to sit down with in person maybe in Melbourne, but keen to hear your thoughts.

Like I mentioned in the PR, this component does work well for all my test sets, just aware that the code could use some clean up, particularly if we want to add features down the line for drawing more complicated pedigrees, or even merge this into seqr one day for better pedigree drawing.
