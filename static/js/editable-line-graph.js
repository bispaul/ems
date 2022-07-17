d3.contextMenu = function(menu, openCallback) {
    // create the div element that will hold the context menu
    d3.selectAll('.d3-context-menu').data([1])
        .enter()
        .append('div')
        .attr('class', 'd3-context-menu');

    // close menu
    d3.select('body').on('click.d3-context-menu', function() {
        d3.select('.d3-context-menu').style('display', 'none');
    });

    // this gets executed when a contextmenu event occurs
    return function(data, index) {
        var elm = this;

        d3.selectAll('.d3-context-menu').html('');
        var list = d3.selectAll('.d3-context-menu').append('ul');
        list.selectAll('li').data(menu).enter()
            .append('li')
            .html(function(d) {
                return d.title;
            })
            .on('click', function(d, i) {
                d.action(elm, data, index);
                d3.select('.d3-context-menu').style('display', 'none');
            });

        // the openCallback allows an action to fire before the menu is displayed
        // an example usage would be closing a tooltip
        if (openCallback) openCallback(data, index);

        // display context menu
        d3.select('.d3-context-menu')
            .style('left', (d3.event.pageX - 2) + 'px')
            .style('top', (d3.event.pageY - 2) + 'px')
            .style('display', 'block');

        d3.event.preventDefault();
    };
};

var removeByAttr = function(arr, attr, value) {
    var i = arr.length;
    while (i--) {
        if (arr[i] &&
            arr[i].hasOwnProperty(attr) &&
            (arguments.length > 2 && arr[i][attr] === value)) {

            arr.splice(i, 1);

        }
    }
    return arr;
};

SimpleGraph = function(elemid, options, datax) {
    // console.log('SimpleGraph datax', datax.length);
    var color = d3.scale.category20();
    datax.forEach(function(d, i) {
        // console.log(d, i, color(i));
        d.visible = true;
        // d.editable = (d.series != 5 ? true : false)
        d.editable = true;
        d.color = color(d.Key);
        d.old = true;
    });

    var lineData = d3.nest()
        .key(function(d) {
            return d.Key;
        })
        .key(function(d) {
            return d.Block_No;
        })
        .entries(datax);

    lineData.forEach(function(d, i) {
        // console.log(d, i, color(i));
        d.visible = true;
        // d.editable = (d.series != 5 ? true : false)
        d.editable = true;
        d.color = color(d.key);
    });

    var xExtent = d3.extent(datax, function(c) {
            return c.Block_No;
        }),
        yExtent = d3.extent(datax, function(c) {
            return c.Total || 0;
        }),
        yMin = yExtent[0],
        yMax = yExtent[1],
        xMin = xExtent[0],
        xMax = xExtent[1];

    var self = this;
    this.pivot = [];
    this.newtspts = [];
    this.datax = datax;
    // this.newmodel = 'CHYBRID';
    // Add interpolator dropdown
    d3.select("#edittype")
        .on("change", function() {
            self.editoption = this.value;
            self.pivot = [];
            if (this.value === 'CreateNew') {
                $('#forecasteditModal').modal('show');
                // Reset the Selection on MOdal Close
                $('#forecasteditModal').on('hidden.bs.modal', function() {
                    $("#edittype").val('Pivot');
                    self.editoption = $("#edittype").val();
                    self.add_line();
                });
                // console.log('lineData', lineData);
                $(".spkey").empty();
                lineData.forEach(function(d, i) {
                    $(".spkey").append($('<option></option>').val(d.key).html(d.key));
                    // $("#spstartblock").append($('<option></option>').val(d.Block_No).html(d.Block_No));
                    // $("#spendblock").append($('<option></option>').val(d.Block_No).html(d.Block_No));
                    // console.log(d.values.key);
                });
                $(".spstartblock").empty();
                $(".spendblock").empty();
                // var blockno = $.unique(datax.map(function(d) { return d.Block_No; }));
                var blockno = [...new Set(datax.map(function(d) { return d.Block_No; }))];
                blockno.forEach(function(d, i) {
                    // console.log(d);
                    $(".spstartblock").append($('<option></option>').val(d).html(d));
                    $(".spendblock").append($('<option></option>').val(d).html(d));
                });
                var template = $('#hidden-template').html();

                $('#AddRowBtn').click(function() {
                    $('#m1').append(template);
                    lineData.forEach(function(d, i) {
                        $('.spkey').last().append($('<option></option>').val(d.key).html(d.key));
                    });
                    var blockno = $.unique(datax.map(function(d) { return d.Block_No; }));
                    blockno.forEach(function(d, i) {
                        // console.log(d);
                        $(".spstartblock").last().append($('<option></option>').val(d).html(d));
                        $(".spendblock").last().append($('<option></option>').val(d).html(d));
                    });
                });
                $('#RemRowBtn').click(function() {
                    $('#m1').children().last().remove();
                });
                $('#SaveBtn').click(function() {
                    var keyarr = [];
                    var startblk = [];
                    var endblk = [];
                    var wtval = [];
                    var rowobjarr = [];
                    var tempdata = [];
                    var newts = [];
                    $(".spkey").each(function() {
                        var currentElement = $(this);
                        keyarr.push(currentElement.val().split(','));
                    });
                    $(".spstartblock").each(function() {
                        var currentElement = $(this);
                        startblk.push(currentElement.val());
                    });
                    $(".spendblock").each(function() {
                        var currentElement = $(this);
                        endblk.push(currentElement.val());
                    });
                    $(".wtval").each(function() {
                        var currentElement = $(this);
                        wtval.push(currentElement.val());
                    });
                    // console.log(keyarr, startblk, endblk, wtval);
                    for (var i = 0; i < keyarr.length; i++) {
                        var rowobj = {};
                        rowobj["Key"] = keyarr[i];
                        rowobj["StartBlock"] = startblk[i];
                        rowobj["EndBlock"] = endblk[i];
                        rowobj["Weight"] = wtval[i];
                        rowobjarr.push(rowobj);
                    };
                    console.log('rowobjarr', rowobjarr);
                    datax.forEach(function(mdItem) {
                        if (mdItem['old']) {
                            // console.log('mdItem', mdItem);
                            rowobjarr.forEach(function(uiItem) {
                                // console.log('uiItem', _.isEqual(uiItem.Key, mdItem.Key), uiItem.Key, mdItem.Key);
                                var tempobj;
                                if (_.isEqual(uiItem.Key, mdItem.Key)) {
                                    tempobj = mdItem;
                                    tempobj['Weight'] = 1;
                                }
                                if (_.isEqual(uiItem.Key, mdItem.Key) && (+mdItem.Block_No >= +uiItem.StartBlock) && (+mdItem.Block_No <= +uiItem.EndBlock)) {
                                    // console.log('isEqual', +uiItem.Weight);
                                    tempobj['Weight'] = +uiItem.Weight || 1;
                                }
                                // console.log('tempobj', tempobj);
                                if (tempobj !== null && typeof tempobj === 'object') {
                                    tempdata.push(tempobj);
                                }
                            });
                        };
                    });
                    // console.log('tempdata', tempdata);
                    var groupByBlock = _.groupBy(tempdata, function(row) {
                        return row.Block_No;
                    });
                    for (var iobj in groupByBlock) {
                        var tempobj = {};
                        var num = 0;
                        var den = 0;
                        var total = 0;
                        for (var i = 0; i < groupByBlock[iobj].length; i++) {
                            tempobj = $.extend(true, {}, groupByBlock[iobj][i]);
                            // console.log(i, groupByBlock[iobj][i]);
                            num = num + groupByBlock[iobj][i]['Total'] * groupByBlock[iobj][i]['Weight'];
                            den = den + groupByBlock[iobj][i]['Weight']
                        }
                        tempobj['Total'] = (den > 0) ? +(num / den).toFixed(2) : 0;
                        // tempobj['forecastdatarow']['Total'] = (num / den).toFixed(2) ? den > 0 : 0;
                        tempobj['Demand_Bias'] = 0;
                        // tempobj['forecastdatarow']['Demand_Bias'] = 0;
                        tempobj['Demand_Forecast'] = tempobj['Total'];
                        // tempobj['forecastdatarow']['Demand_Forecast'] = tempobj['Total'];
                        tempobj['Model_Name'] = 'CHYBRID';
                        // tempobj['forecastdatarow']['Model_Name'] = 'CHYBRID';
                        tempobj['Key'][tempobj['Key'].length - 1] = 'CHYBRID';
                        // tempobj['forecastdatarow']['Key'][tempobj['Key'].length - 1] = this.newmodel;
                        tempobj['color'] = color(tempobj['Key']);
                        tempobj['old'] = false;
                        // console.log(tempobj['color']);
                        newts.push(tempobj);
                    }
                    // console.log('newts', newts);
                    removeByAttr(datax, 'Model_Name', 'CHYBRID');
                    // console.log('datax append', datax.length);
                    newts.forEach(function(row) {
                        datax.push(row);
                    });
                    // console.log('datax append', datax.length);
                    $('#forecasteditModal').modal('hide');
                });
            } // CreateNew
        })
        .selectAll("option")
        .data([
            "Pivot",
            "Shift",
            "Individual",
            "CreateNew"
        ])
        .enter()
        .append("option")
        .attr("value", String)
        .text(String)
        .selectAll("option")
        .attr("selected", function() {
            return true
        });
    this.editoption = d3.select("#edittype").property("value");

    d3.select("#editts")
        .on("change", function(d) {
            // console.log(d, this.value);
            self.pivot = [];
            self.editts = this.value;
            self.update();
        })
        .selectAll("option")
        .data(d3.map(datax, function(d) { return d.Key; }).keys())
        .enter()
        .append("option")
        .attr("value", function(d) { return d; })
        .text(function(d) { return d; })
        .selectAll("option")
        .attr("selected", function() {
            return true
        });
    this.editts = d3.select("#editts").property("value");

    this.chart = document.getElementById(elemid);
    this.cx = this.chart.clientWidth;
    this.cy = this.chart.clientHeight;
    this.options = options || {};
    this.options.xmax = xMax || 96;
    this.options.xmin = xMin || 0;
    this.options.ymax = yMax || 1000;
    this.options.ymin = yMin || 0;
    this.menu = [{
        title: 'Set Pivot Point',
        action: function(elm, d, i) {
            // console.log('Option Selected', self.editoption);
            if (self.editoption === 'Pivot' && self.pivot.length <= 1) {
                self.pivot[0] = d.Block_No;
            } else if (self.editoption === 'Pivot' && self.pivot.length > 1) {
                self.pivot = [];
                self.pivot[0] = d.Block_No;
            } else if (self.editoption === 'Shift' && self.pivot.length >= 2) {
                self.pivot.shift();
                self.pivot.push(d.Block_No);
            } else if (self.editoption === 'Shift' && self.pivot.length < 2) {
                self.pivot.push(d.Block_No);
                // } else if (self.editoption === 'CreateNew') {
                //     self.pivot = [];
                //     $('#editModal').modal('show');
            } else {
                self.pivot = [];
            }
            // console.log('Item #1 clicked!');
            // console.log('The data for this circle is: ', d);
        }
    }];

    this.padding = {
        "top": this.options.title ? 40 : 20,
        "right": 30,
        "bottom": this.options.xlabel ? 60 : 10,
        "left": this.options.ylabel ? 70 : 45
    };

    this.size = {
        "width": this.cx - this.padding.left - this.padding.right,
        "height": this.cy - this.padding.top - this.padding.bottom
    };
    // console.log(this.options.xmin, this.options.xmax, this.options.ymax, this.options.ymin);
    // x-scale
    this.x = d3.scale.linear()
        .domain([this.options.xmin - 1, this.options.xmax])
        .nice()
        .range([0, this.size.width])
        .nice();

    // drag x-axis logic
    this.downx = Math.NaN;

    // y-scale (inverted domain)
    this.y = d3.scale.linear()
        .domain([this.options.ymax + 1, this.options.ymin - 1])
        .nice()
        .range([0, this.size.height])
        .nice();

    // drag y-axis logic
    this.downy = Math.NaN;

    this.dragged = this.selected = null;

    this.line = d3.svg.line()
        .x(function(d, i) {
            // console.log('line', d.values[0].Block_No);
            return self.x(d.values[0].Block_No);
        })
        .y(function(d, i) {
            return self.y(d.values[0].Total || 0);
        })
        .interpolate("monotone");

    this.vis = d3.select(this.chart).append("svg")
        .attr("width", this.cx)
        .attr("height", this.cy)
        .append("g")
        .attr("transform", "translate(" + this.padding.left + "," + this.padding.top + ")");

    this.plot = this.vis.append("rect")
        .attr("width", this.size.width)
        .attr("height", this.size.height)
        .style("fill", "none")
        .attr("id", "mouse-tracker")
        .attr("pointer-events", "all")
        .on("mousedown.drag", self.plot_drag())
        .on("touchstart.drag", self.plot_drag());
    this.plot.call(d3.behavior.zoom().x(this.x).y(this.y).on("zoom", this.redraw()));

    // add Chart Title
    if (this.options.title) {
        this.vis.append("text")
            .attr("class", "axis")
            .text(this.options.title)
            .attr("x", this.size.width / 2)
            .attr("dy", "-0.8em")
            .style("text-anchor", "middle")
            .style("font", "12px sans-serif");
    }

    // Add the x-axis label
    if (this.options.xlabel) {
        this.vis.append("text")
            .attr("class", "axis")
            .text(this.options.xlabel)
            .attr("x", this.size.width / 2)
            .attr("y", this.size.height)
            .attr("dy", "3.8em")
            .style("text-anchor", "middle")
            .style("font", "12px sans-serif");
        // .style("font", "12px sans-serif");
    }

    // add y-axis label
    if (this.options.ylabel) {
        this.vis.append("g").append("text")
            .attr("class", "axis")
            .text(this.options.ylabel)
            .style("text-anchor", "middle")
            .style("font", "12px sans-serif")
            .attr("transform", "translate(" + -60 + " " + this.size.height / 2 + ") rotate(-90)");
    }

    //Creating X and Y axis lines as the normal d3.svg.axis is not used to render the lines
    this.xAxisLineGroup = this.vis.append("g")
        .attr("class", "xAxis-line");

    this.xAxisLineGroup
        .append("line")
        .style("opacity", 1)
        .attr("x1", 0).attr("x2", this.size.width)
        .attr("y1", this.size.height)
        .attr("y2", this.size.height);

    this.yAxisLineGroup = this.vis.append("g")
        .attr("class", "yAxis-line");

    this.yAxisLineGroup
        .append("line")
        .style("opacity", 1)
        .attr("x1", 0).attr("x2", 0)
        .attr("y1", 0).attr("y2", this.size.height);

    this.legend = this.vis.append("g")
        .attr("class", "legend")
        // .attr("x", width/2 + 30)
        .attr("x", this.size.width)
        .attr("y", 25)
        .attr("height", 20)
        .attr("width", 100);

    this.legend.selectAll('g').data(lineData)
        .enter()
        .append('g')
        .each(function(d, i) {
            var g = d3.select(this);
            g.append("text")
                .attr("x", self.size.width / 35 + (i * 100))
                .attr("y", self.size.height + 59)
                // .attr("height", 10)
                // .attr("width", 50)
                .style("font", "10px sans-serif")
                .style("fill", d.color)
                .text(function(d) {
                    // console.log(d);
                    if (d.key.length > 10) {
                        var temp = d.key.split(",");
                        return temp[temp.length - 1].substring(0, 10);
                    } else {
                        d.key;
                    }
                });
        });

    this.viewbox = this.vis.append("svg")
        .attr("top", 0)
        .attr("left", 0)
        .attr("width", this.size.width)
        .attr("height", this.size.height)
        .attr("viewBox", "0 0 " + this.size.width + " " + this.size.height)
        .attr("class", "line");

    this.viewbox.selectAll(".line")
        .data(lineData)
        .enter()
        .append("path")
        .attr("class", "line")
        .attr("stroke-width", 1)
        .attr("fill", "none")
        .style("pointer-events", "none")
        .style("stroke", function(d, i) {
            // console.log('dcolor', d, i);
            return d.color;
        })
        .attr("id", function(d) {
            return "line-" + d.Key; // Give line id of line-(insert issue name, with any spaces replaced with no spaces)
        })
        .attr("d", function(d, i) {
            // console.log('d', d.visible ? self.line(d.values) : null);
            return d.visible ? self.line(d.values) : null;
        });

    d3.select(this.chart)
        .on("mousemove.drag", self.mousemove())
        .on("touchmove.drag", self.mousemove())
        .on("mouseup.drag", self.mouseup())
        .on("touchend.drag", self.mouseup());

    this.redraw()();
};

//
// SimpleGraph methods
//
SimpleGraph.prototype.add_line = function() {
    var self = this;
    // console.log('add_line', self.datax.length, self.datax);
    var color = d3.scale.category20();
    self.datax.forEach(function(d, i) {
        // console.log(d, i, color(i));
        d.visible = true;
        // d.editable = (d.series != 5 ? true : false)
        d.editable = true;
        d.color = color(d.Key);
    });

    var lineData = d3.nest()
        .key(function(d) {
            return d.Key;
        })
        .key(function(d) {
            return d.Block_No;
        })
        .entries(self.datax);

    lineData.forEach(function(d, i) {
        // console.log(d, i, color(i));
        d.visible = true;
        // d.editable = (d.series != 5 ? true : false)
        d.editable = true;
        d.color = color(d.key);
    });

    var xExtent = d3.extent(self.datax, function(c) {
            return c.Block_No;
        }),
        yExtent = d3.extent(self.datax, function(c) {
            return c.Total || 0;
        }),
        yMin = yExtent[0],
        yMax = yExtent[1],
        xMin = xExtent[0],
        xMax = xExtent[1];

    this.x = d3.scale.linear()
        .domain([this.options.xmin - 1, this.options.xmax])
        .nice()
        .range([0, this.size.width])
        .nice();

    // y-scale (inverted domain)
    this.y = d3.scale.linear()
        .domain([this.options.ymax + 1, this.options.ymin - 1])
        .nice()
        .range([0, this.size.height])
        .nice();

    self.viewbox.selectAll(".line")
        .data(lineData)
        .enter()
        .append("path")
        .attr("class", "line")
        .attr("stroke-width", 1)
        .attr("fill", "none")
        .style("pointer-events", "none")
        .style("stroke", function(d, i) {
            // console.log('dcolor', d, i);
            return d.color;
        })
        .attr("id", function(d) {
            return "line-" + d.Key; // Give line id of line-(insert issue name, with any spaces replaced with no spaces)
        })
        .attr("d", function(d, i) {
            // console.log('d', d.visible ? self.line(d.values) : null);
            return d.visible ? self.line(d.values) : null;
        });

    d3.select("#editts")
        .on("change", function(d) {
            // console.log(d, this.value);
            self.editts = this.value;
            self.update();
        })
        .selectAll("option")
        .data(d3.map(self.datax, function(d) { return d.Key; }).keys())
        .enter()
        .append("option")
        .attr("value", function(d) { return d; })
        .text(function(d) { return d; })
        .selectAll("option")
        .attr("selected", function() {
            return true
        });
    this.editts = d3.select("#editts").property("value");

    self.legend.selectAll('g').data(lineData)
        .enter()
        .append('g')
        .each(function(d, i) {
            var g = d3.select(this);
            g.append("text")
                .attr("x", self.size.width / 35 + (i * 100))
                .attr("y", self.size.height + 59)
                // .attr("height", 10)
                // .attr("width", 50)
                .style("font", "10px sans-serif")
                .style("fill", d.color)
                .text(function(d) {
                    // console.log(d);
                    if (d.key.length > 10) {
                        var temp = d.key.split(",");
                        return temp[temp.length - 1].substring(0, 10);
                    } else {
                        d.key;
                    }
                });
        });
    self.update();
};

SimpleGraph.prototype.plot_drag = function() {
    var self = this;
    return function() {
        d3.select('body').style("cursor", "move");
    }
};

SimpleGraph.prototype.update = function() {
    var self = this;
    // console.log('self.datax.length', self.datax);
    self.datax.forEach(function(row) {
        if (row.Key.toString() === self.editts) {
            row.editable = true;
        } else {
            row.editable = false;
        }
    });

    self.viewbox
        .selectAll("path")
        .attr("d", function(d, i) {
            // console.log('d', d);
            return d.visible ? self.line(d.values) : null;
        });

    // console.log('dataxlength', datax.length);
    var circle = self.viewbox.selectAll("circle")
        .data(self.datax);

    circle.enter().append("circle")
        .attr("class", function(d) { return d === self.selected ? "selected" : null; })
        .attr("cx", function(d) { return self.x(d.Block_No); })
        .attr("cy", function(d) { return self.y(d.Total || 0); })
        .attr("r", 2.5)
        .style("cursor", "ns-resize")
        .attr("fill", function(d, i) {
            // console.log('Model_Name', d.Model_Name);
            return d.visible && d.editable ? d.color : "none";
        })
        .on("mouseover", self.handle_mouseover())
        .on("mouseout", self.handle_mouseout())
        .on("mousemove", self.handle_mousemove())
        .on("contextmenu", d3.contextMenu(self.menu))
        .on("mousedown.drag", self.datapoint_drag())
        .on("touchstart.drag", self.datapoint_drag());
    // .on("click", self.handle_mouseclick());

    circle
        .attr("class", function(d) { return d === self.selected ? "selected" : null; })
        .attr("cx", function(d) {
            return self.x(d.Block_No);
        })
        .attr("cy", function(d) { return self.y(d.Total || 0); })
        .attr("fill", function(d, i) {
            return d.visible && d.editable ? d.color : "none";
        });

    circle.exit().remove();
}

SimpleGraph.prototype.handle_mouseover = function() {
    var self = this;
    d3.selectAll('.d3-display').data([1])
        .enter()
        .append('div')
        .attr('class', 'd3-display');
}

SimpleGraph.prototype.handle_mousemove = function() {
    var self = this;
    return function(d, i) {
        var out = _.filter(self.datax, function(val) {
            return val.Block_No === d.Block_No;
        });
        var htmltxt = '';
        out.forEach(function(row) {
            if (row.Model_Name === d.Model_Name) {
                htmltxt = htmltxt + '<font color="' + row.color + '">' + '<u>' + row.Total.toFixed(2) + ' | ' + row.Block_No + ' | ' + row.Model_Name + '</u>' + '</font>' + '<br>';
            } else {
                htmltxt = htmltxt + '<font color="' + row.color + '">' + row.Total.toFixed(2) + ' | ' + row.Block_No + ' | ' + row.Model_Name + '</font>' + '<br>';
            }
        });
        // console.log(htmltxt);
        // Specify where to put label of text
        self.vis.append("text").attr({
                // id: "t" + d.Block_No + "-" + d.Total + "-" + i, // Create an id for text so we can select it later for removing on mouseout
                id: "t" + 123 + "-" + 456 + "-" + i,
                x: function() {
                    return self.x(d.Block_No);
                },
                y: function() {
                    return self.y(d.Total);
                }
            })
            .text(function() {
                d3.selectAll('.d3-display').html(htmltxt);
                // d3.selectAll('.d3-display').html(d.Block_No + ' | ' + d.Total.toFixed(2));
                d3.select('.d3-display')
                    .style('left', (d3.event.pageX - 6) + 'px')
                    .style('top', (d3.event.pageY + 6) + 'px')
                    .style('display', 'block');
                return; // Value of the text
            });
    }
}

SimpleGraph.prototype.handle_mouseout = function() {
    var self = this;
    return function(d, i) {
        // Select text by id and then remove
        d3.select("#t" + 123 + "-" + 456 + "-" + i).remove(); // Remove text location
        d3.select('.d3-display').style('display', 'none');
    }
}

SimpleGraph.prototype.datapoint_drag = function() {
    var self = this;
    return function(d) {
        // Preventing a drag event on right click
        if (d3.event.button === 2) {
            self.dragged = null;
            return;
        }
        document.onselectstart = function() { return false; };
        self.selected = self.dragged = d;
        self.update();
    }
};

SimpleGraph.prototype.mousemove = function() {
    var self = this;
    return function() {
        var p = d3.mouse(self.vis[0][0]),
            t = d3.event.changedTouches;

        if (self.dragged) {
            var curX = self.dragged.Block_No;
            var oldY = self.dragged.Total;
            if (self.editoption === 'Pivot' && self.pivot.length > 0) {
                console.log('pivot', self.pivot[0]);
                // Don't let dragging of the pivot point.
                if (curX === self.pivot[0]) {
                    self.dragged = null;
                } else {
                    if (curX >= self.pivot[0]) {
                        startBlk = self.pivot[0];
                        endBlk = curX;
                    } else {
                        startBlk = curX;;
                        endBlk = self.pivot[0];
                    }
                    var dxx = curX - self.pivot[0];
                    self.dragged.Total = self.y.invert(Math.min(self.size.height, p[1]));
                    var dyy = self.dragged.Total - oldY;

                    others = _.filter(self.datax, function(element) {
                        // console.log('others', element);
                        return _.isEqual(element.Key, self.dragged.Key) && element.Block_No > startBlk && element.Block_No < endBlk;
                    });

                    function movePoints() {
                        _.each(others, function(step) {
                            dcxx = step.Block_No - self.pivot[0];
                            step.Total = step.Total + ((dyy / dxx) * dcxx);
                            step.Demand_Bias = step.Total - step.Demand_Forecast;
                            // console.log('dcxx', dcxx, ((dyy / dxx) * dcxx), step.Block_No, step.Total, step.Demand_Bias);
                        })
                    }
                    movePoints(others);
                    self.update();
                }

            } else if (self.editoption === 'Shift' && self.pivot.length === 2) {
                if (self.pivot[0] < self.pivot[1]) {
                    startBlk = self.pivot[0];
                    endBlk = self.pivot[1];
                } else {
                    startBlk = self.pivot[1];
                    endBlk = self.pivot[0];
                }
                others = _.filter(self.datax, function(element) {
                    // console.log('others', element);
                    return _.isEqual(element.Key, self.dragged.Key) && element.Block_No >= startBlk && element.Block_No <= endBlk;
                });
                if (curX >= startBlk && curX <= endBlk) {
                    // var dyy = d.Total - oldY;
                    var dyy = self.y.invert(Math.min(self.size.height, p[1])) - oldY;
                }
                // console.log('startBlk endBlk:', startBlk, endBlk);
                function movePoints() {
                    _.each(others, function(step) {
                        if (dyy) {
                            step.Total = step.Total + dyy;
                            step.Demand_Bias = step.Total - step.Demand_Forecast;
                        }
                        // console.log('dcxx', dcxx, ((dyy / dxx) * dcxx), step.Block_No, step.Total, step.Demand_Bias);
                    })
                }
                movePoints(others);
                self.update();
            } else if (self.editoption === 'Individual') {
                // self.dragged.y = self.y.invert(Math.max(0, Math.min(self.size.height, p[1])));
                self.dragged.Total = self.y.invert(Math.min(self.size.height, p[1]));
                self.dragged.Demand_Bias = self.dragged.Total - self.dragged.Demand_Forecast;
                self.update();
            };
        }

        if (!isNaN(self.downx)) {
            d3.select('body').style("cursor", "ew-resize");
            var rupx = self.x.invert(p[0]),
                xaxis1 = self.x.domain()[0],
                xaxis2 = self.x.domain()[1],
                xextent = xaxis2 - xaxis1;
            if (rupx != 0) {
                var changex, new_domain;
                // changex = self.downx / rupx;
                changex = (self.downx - xaxis1) / (rupx - xaxis1)
                new_domain = [xaxis1, xaxis1 + (xextent * changex)];
                self.x.domain(new_domain);
                self.redraw()();
            }
            d3.event.preventDefault();
            d3.event.stopPropagation();
        };
        if (!isNaN(self.downy)) {
            d3.select('body').style("cursor", "ns-resize");
            var rupy = self.y.invert(p[1]),
                yaxis1 = self.y.domain()[1],
                yaxis2 = self.y.domain()[0],
                yextent = yaxis2 - yaxis1;
            if (rupy != 0) {
                var changey, new_domain;
                // changey = self.downy / rupy;
                changey = (self.downy - yaxis1) / (rupy - yaxis1)
                new_domain = [yaxis1 + (yextent * changey), yaxis1];
                self.y.domain(new_domain);
                self.redraw()();
            }
            d3.event.preventDefault();
            d3.event.stopPropagation();
        };
    }
}

SimpleGraph.prototype.mouseup = function() {
    var self = this;
    return function() {
        document.onselectstart = function() { return true; };
        d3.select('body').style("cursor", "auto");
        d3.select('body').style("cursor", "auto");
        if (!isNaN(self.downx)) {
            self.redraw()();
            self.downx = Math.NaN;
            d3.event.preventDefault();
            d3.event.stopPropagation();
        };
        if (!isNaN(self.downy)) {
            self.redraw()();
            self.downy = Math.NaN;
            d3.event.preventDefault();
            d3.event.stopPropagation();
        }
        if (self.dragged) {
            // Added for smooth dragging of the end points and updating the data of the end points
            var p = d3.mouse(self.vis[0][0]),
                t = d3.event.changedTouches;
            var curX = self.dragged.Block_No;
            var oldY = self.dragged.Total;
            if (self.editoption === 'Pivot' && self.pivot.length > 0) {
                console.log('pivot', self.pivot[0]);
                if (curX >= self.pivot[0]) {
                    startBlk = self.pivot[0];
                    endBlk = curX;
                } else {
                    startBlk = curX;;
                    endBlk = self.pivot[0];
                }
                var dxx = curX - self.pivot[0];
                self.dragged.Total = self.y.invert(Math.min(self.size.height, p[1]));
                var dyy = self.dragged.Total - oldY;

                others = _.filter(self.datax, function(element) {
                    // console.log('others', element);
                    return _.isEqual(element.Key, self.dragged.Key) && element.Block_No >= startBlk && element.Block_No <= endBlk;
                });

                function movePoints() {
                    _.each(others, function(step) {
                        dcxx = step.Block_No - self.pivot[0];
                        step.Total = step.Total + ((dyy / dxx) * dcxx);
                        step.Demand_Bias = step.Total - step.Demand_Forecast;
                        // console.log('dcxx', dcxx, ((dyy / dxx) * dcxx), step.Block_No, step.Total, step.Demand_Bias);
                    })
                }
                movePoints(others);
                self.update();
            }
            // original code
            self.dragged = null;
        }
    }
}

SimpleGraph.prototype.redraw = function() {
    var self = this;
    return function() {
        var tx = function(d) {
                return "translate(" + self.x(d) + ",0)";
            },
            ty = function(d) {
                return "translate(0," + self.y(d) + ")";
            },
            stroke = function(d) {
                return d ? "#ccc" : "#666";
            },
            fx = self.x.tickFormat(10),
            fy = self.y.tickFormat(10);

        // Regenerate x-ticks…
        var gx = self.xAxisLineGroup.selectAll("g.x")
            .data(self.x.ticks(26), String)
            .attr("transform", tx);

        gx.select("text")
            .text(fx);

        var gxe = gx.enter().insert("g", "a")
            .attr("class", "x")
            .attr("transform", tx);

        gxe.append("line")
            .attr("stroke", stroke)
            .attr("y1", 0)
            .attr("y2", self.size.height);

        gxe.append("g")
            .attr("class", "xticks")
            .append("line")
            // .attr("stroke", '#666')
            .attr("y1", self.size.height)
            .attr("y2", self.size.height + 5);

        gxe.append("text")
            .attr("class", "axis")
            .attr("y", self.size.height)
            .attr("dy", "1.5em")
            .attr("text-anchor", "middle")
            .text(fx)
            .style("font", "12px sans-serif")
            .style("cursor", "ew-resize")
            .on("mouseover", function(d) { d3.select(this).style("font-weight", "bold"); })
            .on("mouseout", function(d) { d3.select(this).style("font-weight", "normal"); })
            .on("mousedown.drag", self.xaxis_drag())
            .on("touchstart.drag", self.xaxis_drag());

        gx.exit().remove();

        // Regenerate y-ticks…
        var gy = self.yAxisLineGroup.selectAll("g.y")
            .data(self.y.ticks(10), String)
            .attr("transform", ty);

        gy.select("text")
            .text(fy);

        var gye = gy.enter().insert("g", "a")
            .attr("class", "y")
            .attr("transform", ty)
            .attr("background-fill", "#FFEEB6");

        gye.append("line")
            .attr("stroke", stroke)
            .attr("x1", 0)
            .attr("x2", self.size.width);

        gye.append("g")
            .attr("class", "yticks")
            .append("line")
            // .attr("stroke", '#666')
            .attr("x1", 0)
            .attr("x2", -5);

        gye.append("text")
            .attr("class", "axis")
            .attr("x", -6)
            .attr("dy", ".35em")
            .attr("text-anchor", "end")
            .text(fy)
            .style("font", "12px sans-serif")
            .style("cursor", "ns-resize")
            .on("mouseover", function(d) { d3.select(this).style("font-weight", "bold"); })
            .on("mouseout", function(d) { d3.select(this).style("font-weight", "normal"); })
            .on("mousedown.drag", self.yaxis_drag())
            .on("touchstart.drag", self.yaxis_drag());

        gy.exit().remove();
        self.plot.call(d3.behavior.zoom().x(self.x).y(self.y).on("zoom", self.redraw()));
        self.update();
    }
}

SimpleGraph.prototype.xaxis_drag = function() {
    var self = this;
    return function(d) {
        document.onselectstart = function() { return false; };
        var p = d3.mouse(self.vis[0][0]);
        self.downx = self.x.invert(p[0]);
    }
}

SimpleGraph.prototype.yaxis_drag = function(d) {
    var self = this;
    return function(d) {
        document.onselectstart = function() { return false; };
        var p = d3.mouse(self.vis[0][0]);
        self.downy = self.y.invert(p[1]);
    }
}