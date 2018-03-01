# Standard library imports
import datetime as dt
import pathlib
import re
import sys

# Third party library imports
from lxml import etree
import pandas as pd
import matplotlib.pyplot as plt


# Fix:
# WMS mapdraws in Sept are zero???
# labels with no folder/service structure
# status codes > 600, like 19627, 498?

def format_func_trillions(value, tick_number):
    return f"{(value / 1e12):.1f}"

def format_func_billions(value, tick_number):
    return f"{int(value / 1e9)}"

def format_func_millions(value, tick_number):
    return f"{int(value / 1e6)}"

def format_func_thousands(value, tick_number):
    return f"{int(value / 1e3)}"

def format_func_trivial(value, tick_number):
    return f"{int(value)}"
        
class Summary(object):
    def __init__(self, project):
        self.project = project
        self.data_root = pathlib.Path.home() / 'data' / 'webstats' / project
        
        # We will write an HTML document for all this.  There will be two
        # pieces, a table of contents and the <DIV> containing all the images.
        # The TOC will link to all the images.
        self.doc = etree.Element('html')
        body = etree.SubElement(self.doc, 'body')
        self.toc = etree.SubElement(body, 'ul')
        
        # Add a paragraph linking in the webalizer output.
        p = etree.SubElement(body, 'p')
        p.text = ("This page contains service-specific web stats only.   "
                  "Overall traffic web stats produced by webalizer can be "
                  "found ")
        
        a = etree.Element('a')
        a.attrib['href'] = (f'http://'
                            f'ncepintradev.ncep.noaa.gov'
                            f'/ncep_common/nowcoast/webalizer'
                            f'/{self.project}'
                            f'/web')
        a.text = 'here'
        a.tail = '.'
        p[:] = [a]
        
        etree.SubElement(body,'hr')
        self.div = etree.SubElement(body, 'div')
       
    def acquire_data(self):
        # Go thru the list of CSV files,  turn into a dataframe
        dfs = []
        keys = []
        for path in self.data_root.glob('*.dat'):
            
            m = re.search('(?P<year>\d{4})(?P<month>\d{2})', path.name)
            file_date = dt.datetime.strptime(m.group(), '%Y%m').date()
            keys.append(file_date)
            
            df = pd.read_csv(path, index_col='service')                       
            dfs.append(df)
        
        keys = pd.DatetimeIndex(keys)
        
        # This gives a dataframe with a MultiIndex of month, service
        self.df = pd.concat(dfs, keys=keys)
    
    def plot_mapdraws(self):
        
        mapdraws = self.df['wms mapdraws'] + self.df['export mapdraws'] \
                 + self.df['wmts mapdraws']
        mapdraws = mapdraws.unstack()
        self.plot(mapdraws,
                  title='Map Draws',
                  ylabel='Map Draws',
                  output_file=f'{self.project}/mapdraws.png')
        
        # Insert the HTML
        li = etree.SubElement(self.toc, 'li')
        a = etree.SubElement(li, 'a')
        a.attrib['href'] = '#mapdraws'
        a.text = 'Mapdraws'
        
        div = etree.SubElement(self.div, 'div')
        a = etree.SubElement(div, 'a')
        a.attrib['name'] = 'mapdraws'
        img = etree.SubElement(div, 'img')
        img.attrib['src'] = 'mapdraws.png'
        etree.SubElement(self.div, 'hr')

    def adjust_ylabel(self, df, ylabel):
        max_val = df.max().max()
        if max_val > 1e12:
            if 'bandwidth' in ylabel:
                ylabel += " (TBytes)"
            else:
                ylabel += " (trillions)"
        elif max_val > 1e9:
            if 'bandwidth' in ylabel:
                ylabel += " (GBytes)"
            else:
                ylabel += " (billions)"
        elif max_val > 1e6:
            if 'bandwidth' in ylabel:
                ylabel += " (MBytes)"
            else:
                ylabel += " (millions)"
        elif max_val > 1e3:
            if 'bandwidth' in ylabel:
                ylabel += " (KBytes)"
            else:
                ylabel += " (thousands)"
        else:
             if 'bandwidth' in ylabel:
                ylabel += " (Bytes)"          
        
        return ylabel.title()
    
    def format_func(self, max_val):
        if max_val > 1e12:
            format_func = format_func_trillions
        elif max_val > 1e9:
            format_func = format_func_billions
        elif max_val > 1e6:
            format_func = format_func_millions
        elif max_val > 1e3:
            format_func = format_func_thousands
        else:
            format_func = format_func_trivial
            
        return format_func
           
    def plot(self, df, title=None, ylabel=None, output_file=None):
        """
        """
        print(f"Plotting {title}...")
        
        # Reorder the columns by maximum value.
        idx = df.max().sort_values(ascending=False).index
        df = df.loc[:, idx]
        
        fig, ax = plt.subplots(figsize=(12, 8))
        
        with plt.style.context('seaborn'):
            df.plot(ax=ax)
        
        ylabel = self.adjust_ylabel(df, ylabel)
        
        # How big is the data, thousands or millions?
        max_val = df.max().max()
        format_func = self.format_func(max_val) 

        ax.yaxis.set_major_formatter(plt.FuncFormatter(format_func))
        
        ax.set_title(title.title())
        ax.set_ylabel(ylabel)
        
        lines, labels = ax.get_legend_handles_labels()
        # split off the nowcoast directory from the labels
        # import pdb; pdb.set_trace()
        labels = [label.split('/')[1] for label in labels]
        ax.legend(lines[:6], labels[:6], loc='best')
        
        fig.savefig(output_file)  
    
    def create_doc_for_latest_month_of_data(self, colname, df):
        """
        Create an HTML table document of the latest month of data.
        """
        # Just take the latest month
        s = df.iloc[-1, :].dropna().sort_values(ascending=False)
        s = s[s > 0]
        s.name = colname
        
        rank = list(range(1, len(s) + 1))
        
        df = pd.DataFrame({'service': s.index, colname: s.values},
                          index=rank)
        
        caption = 'Latest Month of ' + colname
        
        # Create the HTML for the table and then create an entire HTML document
        # out of it.
        formatters = {
            # The numbers are integers, display them as such.
            colname: lambda x: "{:,.0f}".format(int(x)),
            # Don't print the folder name, just the service.
            'service': lambda x: "{0}".format(x.split('/')[1]),
        }
        styles = [
            dict(selector='td', props=[('text-align', 'right')]),
        ]
        table = (df[['service', colname]].style
                                         .set_table_styles(styles)
                                         .set_caption(caption)
                                         .format(formatters)
                                         .render())

        doc = etree.HTML(table)
        
        filename = colname.replace(' ', '_') + '.html'

        etree.ElementTree(doc).write(self.project + '/' + filename,
                                     pretty_print=True)  
        
        return filename
    
    def plot_all(self):
        """
        Plot all the individual columns.  Well, most of them anyway.
        """
        
        p = pathlib.Path(f"{self.project}")
        p.mkdir(exist_ok=True)
        
        for column in self.df.columns:
            
            # Is it a status code column?
            try:
                int(column)
                # If it can be parsed as an integer, then it must be a status
                # code.  We don't want to plot those in this context.
                continue
            except ValueError:
                pass
            
            print(column)
            if column.startswith('error'):
                # Don't do any error plots for the moment.
                continue
            
            output_path = p / f"{column}.png"
            df = self.df[column].unstack()
            
            self.plot(df,
                      title=column,
                      ylabel=column,
                      output_file=str(output_path))
            
            # Insert the HTML:  table of contents entry
            li = etree.SubElement(self.toc, 'li')
            a = etree.SubElement(li, 'a')
            a.attrib['href'] = f'#{column}'
            a.text = column
        
            # Insert the HTML for the image.
            div = etree.SubElement(self.div, 'div')
            a = etree.SubElement(div, 'a')
            a.attrib['name'] = f'{column}'
            img = etree.SubElement(div, 'img')
            img.attrib['src'] = output_path.parts[1]

            # Create the table for the raw data and append a link to it.
            table = self.create_doc_for_latest_month_of_data(column, df)
            a = etree.SubElement(div, 'a')
            a.attrib['href'] = table
            a.text = column
            
            etree.SubElement(self.div, 'hr') 
            
    def append_hits_ranking(self):
        """
        Create HTML for the service hits.
        """
        hits = self.df['hits'].unstack()
        
        # Just take the last month.
        hits = hits.iloc[-1, :].sort_values(ascending=False)
        
        # No NaNs or 0-hit items
        hits = hits.dropna()
        hits = hits[hits > 0]
        
        hits.name = 'hits'
        
        rank = list(range(1, len(hits) + 1))
        
        df = pd.DataFrame({'service': hits.index, 'hits': hits.values},
                          index=rank)
        
        # Create the HTML for the table and then create an ElementTree
        # Element for it.
        html = df.to_html(float_format='%.0f', columns=['service', 'hits'])
        table = etree.fromstring(html)
        
        # Link it into the TOC
        li = etree.SubElement(self.toc, 'li')
        a = etree.SubElement(li, 'a')
        a.attrib['href'] = '#monthlyservicetable'
        a.text = 'monthly service hits (table)'
        
        div = etree.SubElement(self.div, 'div')
        a = etree.SubElement(div, 'a')
        a.attrib['name'] = 'monthlyservicetable'
        
        h2 = etree.SubElement(div, 'h2')
        h2.text = 'Monthly Service Totals'
        
        # And finally add the table into the HTML document.
        div.append(table)
        
    def run(self):

        self.acquire_data()
        self.plot_mapdraws()
        # self.plot_wmts_mapdraws()
        # self.plot_wfs_getfeature_requests()
        self.plot_all()
        
        # self.append_hits_ranking()
        
        etree.ElementTree(self.doc).write(f"{self.project}/index.html",
                                          pretty_print=True)
        
if len(sys.argv) < 2:
    obj = Summary('idpgis')
else:
    obj = Summary(sys.argv[1])
obj.run()
