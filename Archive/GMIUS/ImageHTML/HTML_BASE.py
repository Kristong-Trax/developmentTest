import re

class HTML_Base:
    A = '''
        <html>
            
            <head></head>
            
            <body>
                <div>
                    <style scoped type="text/css">
                        .planogram-compliance {
                            margin: 0px;
                            position: relative;
                        }
            
                        .planogram-compliance .image {
                            position: absolute;
                            margin: 0x;
                            padding: 0px;
                            transform-origin: center;
                        }
            
                        .planogram-compliance .tag {
                            position: absolute;
                            border: 4px solid black;
                            margin: 0x;
                            padding: 0px;
                        }
            
                        .planogram-compliance .tag.status1 {
                            border: 4px solid green;
                        }
            
                        .planogram-compliance .tag.status3 {
                            border: 4px solid red;
                        }
            
                        .planogram-compliance .tag.status2 {
                            border: 4px solid yellow;
                        }
            
                        .planogram-compliance .tag.status0 {
                            border: 4px solid white;
                        }
            
                        .planogram-compliance .tag.status4 {
                            border: 4px dotted white;
                        }
            
                        .planogram-compliance .shelf {
                            background-color: red;
                            position: absolute;
                            border: 1px solid red;
                            height: 2px;
                        }
            
                        .planogram-compliance .bay {
                            background-color: blue;
                            position: absolute;
                            border: 1px solid blue;
                            height: 2px;
                        }
            
                        .planogram-compliance .qat_error {
                            position: absolute;
                            border: 10px solid black;
                            margin: 0x;
                            padding: 0px;
                        }
            
                        .planogram-compliance .qat_error.lines_intersection {
                            border: 30px solid red;
                        }
            
                        .planogram-compliance .qat_error.stitching {
                            border: 30px solid blue;
                        }
            
                        .planogram-compliance .qat_error.detached_probe {
                            border: 30px solid green;
                        }
            
                        .planogram-compliance .qat_error.no_tags {
                            border: 30px solid black;
                        }
            
                        .planogram-compliance .qat_error.tag_on_shelf {
                            border: 30px solid purple;
                        }
            
                        .planogram-compliance .qat_error.rotated_probe {
                            border: 30px solid orange;
                        }
            
                        .planogram-compliance .qat_error.shelves_crossing {
                            border: 30px solid brown;
                        }
            
                        .planogram-compliance .Other {
                            background-color: black;
                        }
                '''

    C = '''
                       div div.planogram-compliance {
                        margin: 0px;
                        display: block;
                        border: 0px;
                        padding: 0px;
                        position: relative;
                        box-sizing: content-box;
                        border: 1px solid black;
                    }
        
                    div div.planogram-compliance>.item {
                        display: block;
                        padding: 0px;
                        white-space: nowrap;
                        line-height: 1px;
                        position: absolute;
                        margin: 0px;
                    }
        
                    div div.planogram-compliance>.bay_divider {
                        display: block;
                        position: absolute;
                        width: 2px;
                        height: 10px;
                        top: -5px;
                        background-color: black;
                    }
        
                    div div.planogram-compliance>.region-label {
                        font-size: 20px;
                        display: block;
                        position: absolute;
                        color: white;
                        font-weight: bold;
                        text-align: center;
                        vertical-align: middle;
                        overflow: hidden;
                    }
                </style>
            '''

    F = '''
                        </div>
                    </div>
                </div>
        </body>
        
        </html>
        
        </html>
    '''
    COLORS = ['green', 'blue', 'purple', 'red', 'orange', 'yellow', 'brown']

    def __init__(self, orig_html, display_attribs):
        self.mult_factor = 1
        self.size = [0, 0]
        self.color_num = 0
        self.num_colors = len(self.COLORS)
        self.clusters = []
        self.products = []
        self.display_attribs = display_attribs
        self.extracted_html = None
        self.D = self.extract_planogram_compliance(orig_html)

    def next_color(self):
        next_color = self.color_num + 1
        if next_color >= self.num_colors:
            next_color = 0
        self.color_num = next_color

    def add_cluster(self, group_num=0, opacity=.6):
        ''' ex:   .planogram-compliance .Group0	{	background-color:	#CD8C95	;	opacity: .7;}'''
        if group_num != -1:
            color = self.COLORS[self.color_num]
            self.next_color()
        else:
            color = 'black'
            opacity = .8
        self.clusters.append('.planogram-compliance .Group{}	{{background-color:	{};	opacity: {};}}'
                            .format(group_num, color, opacity))

    def add_product(self, attribs):
        ''' ex:
        <div class="item Group0" style="top:306px;left:0px;height:163px;width:208px;background-color: no color selected;"
            title="category: General
            brand: GENERAL
            sub_brand:
            product: Irrelevant"></div>'
        '''
        new_prod = '''
                   <div class="item Group{}" style="top:{}px;left:{}px;height:{}px;width:{}px;background-color: no color selected;"
                   title="
                   '''.format(attribs['cluster'], attribs['rect_y']*self.mult_factor,
                              attribs['rect_x']*self.mult_factor,  attribs['h']*self.mult_factor,
                              attribs['w']*self.mult_factor)
        for attrib in self.display_attribs:
            try:
                new_prod += "{}: {}\n".format(attrib, attribs[attrib])
            except:
                pass
        new_prod += '"></div>'
        self.products.append(new_prod)

    def extract_planogram_compliance(self, html):
        self.mult_factor = float(re.findall('transform:scale\((.*?)\);"', html)[0])
        main_planogram = re.findall('<div class="planogram-compliance"(.*?)px"', html)[0]
        size = main_planogram.replace('style="', '').replace('px', '').split(';')
        self.size = [x.split(':')[1] for x in size]

        planogram_html ='{}{}{}'.format('<div class="planogram-compliance"',
                                        main_planogram,
                                        'px;zoom:.125">')
        stitch_html = '\n'.join(['{}{}{}'.format('<div class="image"', img, '</div>')
                                for img in re.findall('<div class="image"(.*?)</div>', html)])
        shelf_bay_style = '{}{}{}'.format('<div style="top:', re.findall('div style="top:(.*?)\)', html)[0], ');">')
        shelves_html = '\n'.join(['{}{}{}'.format('<div class="shelf"', img, '</div>')
                                        for img in re.findall('<div class="shelf"(.*?)</div>', html)])
        bays_html = '\n'.join(['{}{}{}'.format('<div class="bay"', img, '</div>')
                                        for img in re.findall('<div class="bay"(.*?)</div>', html)])
        bays_html += '</div>'
        return '\n'.join([planogram_html, stitch_html, shelf_bay_style, shelves_html, bays_html])

    def return_html(self):
        B = '\n'.join(self.clusters)
        E = '\n'.join(reversed(self.products))
        return '\n'.join([self.A, B, self.C, self.D, E, self.F])

