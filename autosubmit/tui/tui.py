from textual.app import App, ComposeResult
from textual.widgets import Header, Footer


class AutosubmitTui(App):
    """A Textual app to manage Autosubmit experiments."""

    BINDINGS = [
        ('d', 'toggle_dark', 'Toggle dark mode')
    ]

    def compose(self) -> ComposeResult:
        """Create child widgets for the app."""
        yield Header()
        yield Footer()

    def action_toggle_dark(self) -> None:
        """An actino to goggle dark mode."""
        self.dark = not self.dark


def main() -> None:
    app = AutosubmitTui()
    app.run()


if __name__ == '__main__':
    main()
