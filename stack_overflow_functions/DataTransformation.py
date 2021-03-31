# Data tranformation module
## Contains the functions to transform the data 
## And analyse it
from sparknlp.base import *
from sparknlp.annotator import *
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
from bokeh.io import output_notebook, show, output_file
from bokeh.plotting import figure
from bokeh.models import GeoJSONDataSource, LinearColorMapper, ColorBar
from bokeh.palettes import brewer
import geopandas as gpd

def create_document_cleaner(input_col, clean_up_patterns, lowercase=True):
    """
    Creates a pipeline to remove some text patterns in a column
    args:
        input_col(str) : The string column to clean
        clean_up_patterns(list): list of the regex pattern to clean
        lowercase(bool): If true, lowercase the text
    return:
        docPatternRemoverPipeline(ml.pipeline): The pipeline to
                                                remove the text
    """
    documentAssembler = DocumentAssembler() \
        .setInputCol(input_col) \
        .setOutputCol('_intermediate_results')

    doc_norm = DocumentNormalizer() \
        .setInputCols("_intermediate_results") \
        .setOutputCol(input_col + "_cleaned") \
        .setAction("clean") \
        .setPatterns(clean_up_patterns) \
        .setReplacement(" ") \
        .setPolicy("pretty_all") \
        .setLowercase(True)

    docPatternRemoverPipeline = \
      Pipeline() \
        .setStages([
            documentAssembler,
            doc_norm])
    return docPatternRemoverPipeline

def udf_detect_language(lst_languages):
    """
    Creates a column language based on list of language
    If the tag is in the list of language (lst_languages)
    As pyspark udf does not accept multiple args, we use
    nested functions
    """
    def identify_language(tags):
        """
        Identify the language based on a list of tags
        args:
            lst_languages(list): List of the languages
        return:
            language(str): The language in the tags
        """

        # Assumption : The first language found in the tags is the most important
        for tag in tags:
            for language in lst_languages:
                if language == tag:
                    return language
        return None
    return F.udf(identify_language, StringType())

def plot_map(json_data, title, mapping_field, min_value, max_value,
             n_colors=6, plot_height=600, plot_width=950, palette_name='RdYlBu',
             reverse_palette=False):
    """
    Creates a country map to display on a notebook with a continuous variable as a color  
    """
    geosource = GeoJSONDataSource(geojson = json_data)
    
    # Define a sequential multi-hue color palette.
    palette = brewer[palette_name][n_colors]
    
    # Reverse color order so that dark blue is highest obesity.
    if reverse_palette:
        palette = palette[::-1]
    
    # Instantiate LinearColorMapper that linearly maps numbers in a range, into a sequence of colors.
    color_mapper = LinearColorMapper(palette = palette, low = min_value, high = max_value)

    # Create color bar. 
    color_bar = ColorBar(color_mapper=color_mapper, width = 500, height = 20,
    border_line_color=None, location = (0,0), orientation = 'horizontal')
    
    # Create figure object.
    p = figure(title = title, plot_height = plot_height , plot_width = plot_width, toolbar_location = None)
    p.xgrid.grid_line_color = None
    p.ygrid.grid_line_color = None
    
    # Add patch renderer to figure. 
    p.patches('xs','ys', source = geosource,fill_color = {'field' : mapping_field, 'transform' : color_mapper},
              line_color = 'black', line_width = 0.25, fill_alpha = 1)
    
    # Specify figure layout.
    p.add_layout(color_bar, 'below')
    
    # Display figure inline in Jupyter Notebook.
    output_notebook()
    
    # Display figure.
    return p
    
