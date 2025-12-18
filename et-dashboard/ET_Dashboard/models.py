"""
Data models for the EasyTrack Dashboard application.

This module contains data transfer objects and domain models used
throughout the dashboard application.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class EnhancedDataSource:
    """
    A wrapper class for data source objects with optional plot visualization.

    This class enhances a basic data source dictionary with visualization
    capabilities by attaching plotly plot strings.

    Attributes:
        db_data_source: Dictionary containing data source information with
            keys 'id', 'name', and 'icon_name'.
        plot_str: Optional HTML string containing a plotly visualization.
    """

    db_data_source: Dict[str, Any]
    plot_str: Optional[str] = field(default=None)

    def attach_plot(self, plot_str: str) -> None:
        """
        Attach a plot visualization to this data source.

        Args:
            plot_str: HTML string containing the plotly plot.
        """
        self.plot_str = plot_str

    @property
    def id(self) -> int:
        """Get the data source ID."""
        return self.db_data_source["id"]

    @property
    def name(self) -> str:
        """Get the data source name."""
        return self.db_data_source["name"]

    @property
    def icon_name(self) -> str:
        """Get the data source icon name."""
        return self.db_data_source["icon_name"]

    @property
    def plot(self) -> Optional[str]:
        """Get the attached plot HTML string."""
        return self.plot_str

