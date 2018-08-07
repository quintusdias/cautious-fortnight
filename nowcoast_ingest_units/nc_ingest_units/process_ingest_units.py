import datetime as dt
import itertools
import pathlib

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

class ProcessIngestUnits(object):
    """
    Attributes
    ==========
    root : path
        Search directory tree under this path.
    data : list
        Ingest extents parsed from each file
    time_range : tuple of datetimes
        Upper and lower bounds in time for data
    """
    
    def __init__(self, directory, time_range, side, exclude):
        """

        """

        self.root = pathlib.Path(directory)
        self.data = []
        self.time_range = time_range

        if exclude is None:
            self.exclude = []
        else:
            self.exclude = exclude

        self.side1 = [
            'chazo_cfire_dfire', 'maxt_mint', 'atemp_temp', 'sky_waveh', 'goes_vis', 
            '1hr', '3hr', '6hr', '12hr', 'gmgsi_vis', 'gmgsi_sir', 
            'cbofs', 'creofs', 'leofs', 'lhofs', 'negofs', 'nwgofs',
            'rtofs', 'sjrofs',
            'rtgssthr', 'sfc',  'warnings', 'akcoastal', 
        ]
        self.side2 = [
            'dewpt_rhm', 'pop12_qpf_snow', 'goes_i2', 'goes_i3', 'goes_i4',
            'wgust_wind', '24hr', '48hr', '72hr', 'rtma', 'gmgsi_lir', 
            'dbofs', 'estofs', 'gomofs', 'lmofs', 'loofs', 'lsofs', 'ngofs',
            'nyofs', 'sfbofs', 'tbofs', 'sport', 'lightning', 'rtma', 
            'hazards', 'basereflect'
        ]

        if side is not None:
            if side == 1:
                self.exclude.extend(self.side2)
            else:
                self.exclude.extend(self.side1)
        

    def run(self):

        g = self.root.rglob('*.log')
        for path in g:
            self.process_log(path)

        df = pd.DataFrame(self.data)
        df = df.sort_values(by='start')
        self.plot(df)

    def plot(self, df):
        fig, ax = plt.subplots(figsize=(8,4)) 
        unique_ingests = df.ingest.unique()
        print(unique_ingests)

        # Map the ingests to a unique color and hatch pattern
        n_colors = min(unique_ingests.shape[0], 7)
        colors = sns.color_palette(n_colors=n_colors)

        hatches = (None, r'\\\\', '+', '-', 'O')

        # cmap = {ingest: color for ingest, color in zip(unique_ingests, colors)}
        cmap = {}
        color_it = itertools.cycle(colors)
        for idx, ingest in enumerate(unique_ingests):
            color = next(color_it)
            print(idx)
            cmap[ingest] = {
                'color': color,
                'hatch': hatches[idx // len(colors)]
            }

        actives = {}
        coords = []

        for idx, row in df.iterrows():

            # Print the current status.
            print('Before:')
            for idx, item in actives.items():
                if item is not None:
                    print(f"{idx}: ==> {item['ingest']:20s} {item['start']} : {item['end']}")

            # How many ingests are currently active?
            current_level = 0
            if row['start'] > dt.datetime(2018,2,12,5,31) and row['end'] < dt.datetime(2018,2,12,5,40):
                print(row)
            #if row['ingest'] in ['1hr']:
            #    print(row)
            #    import pdb; pdb.set_trace()

            for idx, active in sorted(actives.items()):

                if active is None:
                    break
                elif row['start'] > active['end']:
                    break
                else:
                    current_level += 1

            # Compute the patch details
            try:
                patch_coords = {
                    'x': row['start'].timestamp(), 
                    'y': current_level, 
                    'width': row['end'].timestamp() - row['start'].timestamp(),
                    'height': 1,
                    'facecolor': cmap[row['ingest']]['color'],
                    'hatch': cmap[row['ingest']]['hatch'],
                    'label': row['ingest'],
                }
            except KeyError:
                import pdb; pdb.set_trace()
                pass
            coords.append(patch_coords)
            actives[current_level] = row
    
            # Purge everything from the actives that have a stop time less than
            # the current start time.
            pop_these = []
            for idx, active_row in actives.items():
                if active_row is None:
                    continue
                if row['start'] > active_row['end']:
                    pop_these.append(idx)
            for idx in pop_these:
                actives.pop(idx)

            # Are there any gaps?
            for idx in range(max(actives.keys())):
                if idx not in actives.keys():
                    # import pdb; pdb.set_trace()
                    actives[idx] = None
    

        encountered_labels = []

        for idx, patch_coords in enumerate(coords):
            xy = patch_coords['x'], patch_coords['y']
            width = patch_coords['width']
            height = patch_coords['height']
            facecolor = patch_coords['facecolor']
            # print(xy, width, height)

            # See https://stackoverflow.com
            # /questions/19385639/duplicate-items-in-legend-in-matplotlib
            label = patch_coords['label']
            if label in encountered_labels:
                label = '_nolegend_'
            else:
                encountered_labels.append(label)

            p = mpatches.Rectangle(xy, width, height, facecolor=facecolor,
                                   label=label)
            p.set_hatch(patch_coords['hatch'])
            ax.add_patch(p)
    
        xmin = min([pc['x'] for pc in coords])
        xmax = max([pc['x'] + pc['width'] for pc in coords])
        ymin = min([pc['y'] for pc in coords])
        ymax = max([pc['y'] + pc['height'] for pc in coords])
        ax.set_xlim(left=xmin, right=xmax)
        ax.set_ylim(bottom=ymin, top=ymax)
    
        lgd = ax.legend(loc='center left', bbox_to_anchor=(1.05, 0.5))

        # Shrink by 20%
        box = ax.get_position()
        ax.set_position([box.x0, box.y0, box.width*0.8, box.height])
    
        xticks = ax.get_xticks()
        # print(xticks)
        xticklabels = [
            dt.datetime.fromtimestamp(xtick).strftime('%H:%M')
            for xtick in xticks
        ]
        ax.set_xticklabels(xticklabels)

        # fig.tight_layout()
    
        plt.show()

    def extract_ingest_name_from_path(self, path):
        """
        Determine the identifier for the ingest from the log.
        """
        parts = path.stem.split('-')
        if len(parts) == 2:
            # E.g. rtgssthr/cron-Thu0450.log
            # 
            # Take the ingest from the parent directory.
            ingest = path.parent.stem

        if len(parts) > 3:
            # CPRK:  e.g. cron-ngofs-21Z-Tue2205.log
            ingest = parts[1]

        if len(parts) == 3:
            # E.g. ['cron', 'estofs', 'Mon2146']
            ingest = parts[1]

        return ingest

    def process_ingest_times(self, path, ingest):
        """
        Process all ingest time extents from the file found at the given path.
        """
        start = None
        command_succeeded = False
        for line in path.open(mode='rt'):
            # if line.startswith('Command Succeeded.'):
            if line.startswith('Executing at'):
                # It is only if there is a line starting with this string that
                # we know that arcpy was fed something.
                command_succeeded = True

            if not line.startswith('=========='):
                continue
            if 'start' in line:
                s = ' '.join(line.split()[4:6])
                start = dt.datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
                if start < self.time_range[0] or start > self.time_range[1]:
                    start = None
                    continue
                print(f"\tExtracted start at {start}")
            
            if 'end' in line and start is not None and command_succeeded:
                s = ' '.join(line.split()[4:6])
                end = dt.datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
                self.data.append(dict(ingest=ingest, start=start, end=end))
                print(f'\tAccepted {ingest} at {start}:{end}')

                start = None
                command_succeeded = False

    def process_log(self, path):
        """
        Determine if a log file falls within our window of interest.  If so,
        parse out the start and stop times of the ingest.

        Parameters
        ----------
        path
            Path to nowCOAST log file
        """
        # Some paths should always be skipped.
        if '.snapshot' in str(path):
            return
        if 'wine' in str(path):
            return

        ingest = self.extract_ingest_name_from_path(path)

        if ingest in self.exclude:
            print(f'excluded {ingest}')
            return

        self.process_ingest_times(path, ingest)
