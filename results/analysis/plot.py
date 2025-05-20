import os 

class FigSaver: 
    def __init__(self, fig_dir: str):
        self.fig_dir = fig_dir
        os.makedirs(fig_dir, exist_ok=True)

    def save_fig(self, fig, name: str):
        fig.savefig(
            os.path.join(self.fig_dir, name),
            dpi=300,
            format='pdf',
            bbox_inches='tight'
                )
        
