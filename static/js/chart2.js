function splineChart() {

    // All options that should be accessible to caller

    var margin = {top: 40, right: 60, bottom: 60, left: 70},
        width = 960 - margin.left - margin.right,
        height = 500 - margin.top - margin.bottom;
    var barPadding = 1;
    var fillColor = 'coral';
    var datax = [];

    var updateWidth;
    var updateHeight;
    var updateFillColor;
    var updateData;

    function chart(selection){
        selection.each(function () {
            var xExtent = d3.extent(datax, function(c) { return c.Block_No(); }),
            yExtent = d3.extent(datax, function(c) { return c.Total(); }),
            yMin = yExtent[0], yMax = yExtent[1], xMin = xExtent[0], xMax = xExtent[1];            
            
            var findMaxY = function (datax) { 
              d3.max(datax, function(c) {
                if (c.visible) return c.Total();
              });
            }    

            var findMinY = function (datax) {
              d3.min(datax, function(c) {
                if (c.visible) return c.Total();
              });
            }

            var x = d3.scale.linear()
              .domain([xMin - 1, xMax])
              .nice()
              .range([0, width])
              .nice()
              ;

            var y = d3.scale.linear()
              .domain([yMax + 1, yMin - 1])
              .nice()
              .range([0, height])
              .nice()
              ;          
            // var barSpacing = height / data.length;
            // var barHeight = barSpacing - barPadding;
            // var maxValue = d3.max(data);
            // var widthScale = width / maxValue;

            var dom = d3.select(this);
            var svg02 = dom.append("svg")
              .attr("width", width + margin.left + margin.right)
              .attr("height", height + margin.top + margin.bottom)
              .append("g")
              .attr("transform", "translate(" + margin.left + "," + margin.top + ")")
              ;

              //Bug in ZOOM functionality fix it update resets the zoom and cancels the scaling
            var plot = svg02.append("rect")
              .attr("width", width)
              .attr("height", height)
              .attr("fill", "none")
              .attr("id", "mouse-tracker")
              .attr("pointer-events", "all")
              .call(d3.behavior.zoom().x(x).y(y).on("zoom", redraw))
              .on("mousedown", function() {
                d3.select('body').style("cursor", "move");
              });

          // add Chart Title
          // if (this.options.title) {
            svg02.append("text")
                .attr("class", "axis")
                .text('Chart Title')
                .attr("x", width/2)
                .attr("dy","-0.8em")
                .style("text-anchor","middle");
          // }

          // Add the x-axis label
          // if (this.options.xlabel) {
            svg02.append("text")
                .attr("class", "axis")
                .text('X Axis')
                .attr("x", width/2)
                .attr("y", height)
                .attr("dy","2.4em")
                .style("text-anchor","middle");
          // }

          // add y-axis label
          // if (this.options.ylabel) {
            svg02.append("g").append("text")
                .attr("class", "axis")
                .text('Y Axis')
                .style("text-anchor","middle")
                .attr("transform","translate(" + -40 + " " + height/2+") rotate(-90)");
          // }

          //Creating X and Y axis lines as the normal d3.svg.axis is not used to render the lines
          var xAxisLineGroup = svg02.append("svg:g")
                    .attr("class", "xAxis-line");

              xAxisLineGroup
              .append("svg:line")
              .style("opacity", 1)
              // .attr('stroke', '#666')
              // .attr('stroke-width', 1.5)
              .attr("x1", 0).attr("x2", width)
              .attr("y1", height).attr("y2", height);

          var yAxisLineGroup = svg02.append("svg:g")
                    .attr("class", "yAxis-line");

              yAxisLineGroup
              .append("svg:line")
              .style("opacity", 1)
              // .attr('stroke', '#666')
              // .attr('stroke-width', 1.5)
              .attr("x1", 0).attr("x2", 0)
              .attr("y1", 0).attr("y2", height);                

          // var drag = d3.behavior.drag()
          //             .origin(function(d) { console.log('origin', d);return d; })
          //             .on("dragstart", dragstarted)
          //             .on("drag", dragged)
          //             .on("dragend", dragended);    
                                          

          var dragged = null,
              selected = null;

          var viewbox = svg02.append("svg")
                          .attr("top", 0)
                          .attr("left", 0)
                          .attr("width", width)
                          .attr("height", height)
                          .attr("viewBox", "0 0 "+width+" "+height)
                          .attr("class", "line")
                          ;

          var line = d3.svg.line()
                      .x(function(d,i) { //console.log('line', d.values[0].Block_No()); 
                        return  x(d.values[0].Block_No()); })
                      .y(function(d,i) { return  y(d.values[0].Total()); })
                      .interpolate("monotone");

          var lines = viewbox.selectAll(".line")
                      .data(lineData)
                      .enter()
                      .append("path")
                      .attr("class","line")
                      .attr("stroke-width", 1)
                      .attr("fill", "none")
                      .style("pointer-events", "none")
                      .style("stroke", function(d,i){
                        // console.log('dcolor', d, i);
                        return d.color;
                      })
                      .attr("id", function(d) {
                        return "line-" + d.key; // Give line id of line-(insert issue name, with any spaces replaced with no spaces)
                      })                  
                      .attr("d", function(d,i) {
                          // console.log('d', d, i);   
                          // return line(d.data); 
                          return d.visible ? line(d.values) : null;  
                      })
                      // .on('mouseover', function(d, i) {
                      //   console.log('mouseover',d, i);
                      //   mouseover(d);
                      // })
                      // .on('mouseover', mouseover)                  
                      // .call(update)
                      ;    

            // var svg = dom.append('svg')
            //     .attr('class', 'bar-chart')
            //     .attr('height', height)
            //     .attr('width', width)
            //     .style('fill', fillColor);

            // var bars = svg.selectAll('rect.display-bar')
            //     .data(data)
            //     .enter()
            //     .append('rect')
            //     .attr('class', 'display-bar')
            //     .attr('y', function (d, i) { return i * barSpacing;  })
            //     .attr('height', barHeight)
            //     .attr('x', 0)
            //     .attr('width', function (d) { return d * widthScale; });


            // update functions
            updateWidth = function() {
                widthScale = width / maxValue;
                bars.transition().duration(1000).attr('width', function(d) { return d * widthScale; });
                svg.transition().duration(1000).attr('width', width);
            };

            updateHeight = function() {
                barSpacing = height / data.length;
                barHeight = barSpacing - barPadding;
                bars.transition().duration(1000).attr('y', function(d, i) { return i * barSpacing; })
                    .attr('height', barHeight);
                svg.transition().duration(1000).attr('height', height);

            };

            updateFillColor = function() {
                svg.transition().duration(1000).style('fill', fillColor);
            };

            updateData = function() {
                barSpacing = height / data.length;
                barHeight = barSpacing - barPadding;
                maxValue = d3.max(data);
                widthScale = width / maxValue;

                var update = svg.selectAll('rect.display-bar')
                    .data(data);

                update
                    .transition()
                    .duration(1000)
                    .attr('y', function(d, i) { return i * barSpacing; })
                    .attr('height', barHeight)
                    .attr('x', 0)
                    .attr('width', function(d) { return d * widthScale; });

                update.enter()
                    .append('rect')
                    .attr('class', 'display-bar')
                    .attr('y', function(d, i) { return i * barSpacing; })
                    .attr('height', barHeight)
                    .attr('x', 0)
                    .attr('width', 0)
                    .style('opacity', 0)
                    .transition()
                    .duration(1000)
                    .delay(function(d, i) { return (data.length - i) * 40; })
                    .attr('width', function(d) { return d * widthScale; })
                    .style('opacity', 1);

                update.exit()
                    .transition()
                    .duration(650)
                    .delay(function(d, i) { return (data.length - i) * 20; })
                    .style('opacity', 0)
                    .attr('height', 0)
                    .attr('x', 0)
                    .attr('width', 0)
                    .remove();
            }

        });
    }

    chart.width = function(value) {
        if (!arguments.length) return width;
        width = value;
        if (typeof updateWidth === 'function') updateWidth();
        return chart;
    };

    chart.height = function(value) {
        if (!arguments.length) return height;
        height = value;
        if (typeof updateHeight === 'function') updateHeight();
        return chart;
    };

    chart.fillColor = function(value) {
        if (!arguments.length) return fillColor;
        fillColor = value;
        if (typeof updateFillColor === 'function') updateFillColor();
        return chart;
    };

    chart.data = function(value) {
        if (!arguments.length) return data;
        data = value;
        if (typeof updateData === 'function') updateData();
        return chart;
    };

    return chart;
}
