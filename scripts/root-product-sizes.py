#!/usr/bin/env python3
"""Print uncompressed and compressed sizes of all branches in an art ROOT file.

Usage:
    python branch_sizes.py <file.root> [tree_name]

Arguments:
    file.root   Path to the art ROOT file
    tree_name   TTree to inspect (default: Events)
"""

import sys
import ROOT

ROOT.gROOT.SetBatch(True)
ROOT.gErrorIgnoreLevel = ROOT.kError


def branch_sizes(filepath, treename="Events"):
    f = ROOT.TFile.Open(filepath, "READ")
    if not f or f.IsZombie():
        print(f"Error: could not open {filepath}", file=sys.stderr)
        sys.exit(1)

    tree = f.Get(treename)
    if not tree:
        print(f"Error: tree '{treename}' not found in {filepath}", file=sys.stderr)
        f.Close()
        sys.exit(1)

    branches = list(tree.GetListOfBranches())
    if not branches:
        print("No branches found.")
        f.Close()
        return

    rows = []
    for b in branches:
        uncomp = b.GetTotBytes("*") / 1024**2
        comp   = b.GetZipBytes("*") / 1024**2
        ratio  = uncomp / comp if comp > 0 else float("inf")
        rows.append((uncomp, comp, ratio, b.GetName()))

    rows.sort(reverse=True)

    nevents = tree.GetEntries()

    col_w = max(len(r[3]) for r in rows)
    header = (
        f"{'Branch':<{col_w}}  {'Uncomp (MB)':>12}  {'Comp (MB)':>10}  {'Ratio':>6}"
        f"  {'MB/evt (uncomp)':>15}  {'MB/evt (comp)':>13}"
    )
    print(f"File   : {filepath}")
    print(f"Tree   : {treename}  ({len(rows)} branches)")
    print(f"Events : {nevents}")
    print("-" * len(header))
    print(header)
    print("-" * len(header))
    for uncomp, comp, ratio, name in rows:
        ratio_str = f"{ratio:6.1f}x" if ratio != float("inf") else "   n/a"
        mb_uncomp = uncomp / nevents if nevents > 0 else float("nan")
        mb_comp   = comp   / nevents if nevents > 0 else float("nan")
        print(
            f"{name:<{col_w}}  {uncomp:>12.3f}  {comp:>10.3f}  {ratio_str}"
            f"  {mb_uncomp:>15.6f}  {mb_comp:>13.6f}"
        )
    print("-" * len(header))

    total_uncomp = sum(r[0] for r in rows)
    total_comp   = sum(r[1] for r in rows)
    total_ratio  = total_uncomp / total_comp if total_comp > 0 else float("inf")
    total_mb_uncomp = total_uncomp / nevents if nevents > 0 else float("nan")
    total_mb_comp   = total_comp   / nevents if nevents > 0 else float("nan")
    print(
        f"{'TOTAL':<{col_w}}  {total_uncomp:>12.3f}  {total_comp:>10.3f}  {total_ratio:6.1f}x"
        f"  {total_mb_uncomp:>15.6f}  {total_mb_comp:>13.6f}"
    )

    f.Close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    filepath  = sys.argv[1]
    treename  = sys.argv[2] if len(sys.argv) > 2 else "Events"
    branch_sizes(filepath, treename)
    