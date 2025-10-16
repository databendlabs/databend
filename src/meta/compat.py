import semantic_version
import matplotlib.pyplot as plt
import matplotlib.patches as patches

meta_to_query_compat = [
    # upto meta version(exclusive) : compatible query version [from, to) (left closed, right open)
    "0.0.0     0.0.0   0.0.0   ",
    "0.8.30    0.0.0   0.0.0   ",
    "0.8.35    0.7.59  0.8.80  ",
    "0.9.23    0.7.59  1.1.34  ",
    "0.9.42    0.8.80  1.1.34  ",
    "1.1.32    0.9.41  1.1.34  ",
    "1.2.226   0.9.41  1.2.287 ",
    "1.2.258   0.9.41  1.2.361 ",
    "1.2.663   0.9.41  1.2.726 ",
    "1.2.677   1.2.287 1.2.726 ",
    "1.2.755   1.2.676 1.2.821 ",
    "1.2.756   1.2.676 1.2.821 ",
    "1.2.764   1.2.676 1.2.821 ",
    "1.2.768   1.2.676 1.2.821 ",
    "1.2.770   1.2.676 1.2.823 ",
    "∞         1.2.676 ∞       ",
]


def draw_meta_to_query_compat(output_file: str):
    fig, ax = plt.subplots(figsize=(12, 8))

    # Parse compatibility data and collect all unique versions
    ranges = []
    all_meta_versions = set()
    all_query_versions = set()

    for i, line in enumerate(meta_to_query_compat):
        if line.strip():
            meta, q_from, q_to = line.strip().split()

            # Skip empty ranges
            if q_from == "0.0.0" and q_to == "0.0.0":
                continue

            ranges.append((meta, q_from, q_to))
            all_meta_versions.add(meta)
            all_query_versions.add(q_from)
            all_query_versions.add(q_to)

    # Sort versions and create position mappings
    def version_key(v):
        if v == "∞":
            return (999, 999, 999)
        ver = semantic_version.Version(v)
        return (ver.major, ver.minor, ver.patch)

    sorted_meta_versions = sorted(all_meta_versions, key=version_key)
    sorted_query_versions = sorted(all_query_versions, key=version_key)

    # Create position mappings
    meta_pos = {v: i for i, v in enumerate(sorted_meta_versions)}
    query_pos = {v: i for i, v in enumerate(sorted_query_versions)}

    # Create compatibility rectangles
    for i, (meta, q_from, q_to) in enumerate(ranges):
        # Get positions
        meta_end_x = meta_pos[meta]  # This is the exclusive upper bound
        q_from_y = query_pos[q_from]
        q_to_y = query_pos[q_to]

        # Determine the start position (previous meta version or 0)
        meta_idx = sorted_meta_versions.index(meta)
        if meta_idx > 0:
            meta_start_x = meta_pos[sorted_meta_versions[meta_idx - 1]]
        else:
            meta_start_x = -0.5  # Start from beginning for first range

        # Width of the rectangle (up to but not including current meta version)
        width = meta_end_x - meta_start_x

        # Height of the rectangle ([from, to) range - covers q_from_y to just before q_to_y)
        height = q_to_y - q_from_y

        # Create rectangle patch
        rect = patches.Rectangle(
            (meta_start_x, q_from_y),
            width,
            height,
            linewidth=1,
            edgecolor="darkgreen",
            facecolor="lightgreen",
            alpha=0.7,
        )
        ax.add_patch(rect)

    # Set up the plot
    ax.set_xlim(-0.5, len(sorted_meta_versions) - 0.5)
    ax.set_ylim(-0.5, len(sorted_query_versions) - 0.5)
    ax.set_xlabel("Meta Version", fontsize=12, fontweight="bold")
    ax.set_ylabel("Compatible With Query", fontsize=12, fontweight="bold")
    ax.set_title(
        "Databend-Meta/Query Version Compatibility Chart",
        fontsize=14,
        fontweight="bold",
    )

    # Add grid
    ax.grid(True, alpha=0.3)

    # Set ticks with version labels
    ax.set_xticks(range(len(sorted_meta_versions)))
    ax.set_xticklabels(
        [v if v != "∞" else "+∞" for v in sorted_meta_versions], rotation=45
    )

    ax.set_yticks(range(len(sorted_query_versions)))
    ax.set_yticklabels([v if v != "∞" else "+∞" for v in sorted_query_versions])

    # Add legend
    legend_patch = patches.Patch(color="lightgreen", label="Compatible Query Versions")
    ax.legend(handles=[legend_patch], loc="upper left")

    # Save the plot
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    plt.close()

    print(f"Compatibility chart saved to {output_file}")


if __name__ == "__main__":
    output_file = "compatibility_chart.png"
    draw_meta_to_query_compat(output_file)
    print(f"Chart generated: {output_file}")
