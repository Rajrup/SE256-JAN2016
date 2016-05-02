<%--
  Created by IntelliJ IDEA.
  User: robin
  Date: 30/4/16
  Time: 10:41 AM
  To change this template use File | Settings | File Templates.
--%>
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<html>
<head>
    <title>Sentiment</title>
    <script type="text/javascript" src="https://www.gstatic.com/charts/loader.js"></script>
    <script type="text/javascript" src="https://www.google.com/jsapi"></script>
    <script type="text/javascript">
        google.charts.load('current', {'packages':['geochart']});
        google.charts.setOnLoadCallback(drawMap);
        var buffers=[];
        var charts=[];
        var drawingBuffer=0;
        function parse(val) {
            var result = "Not found",
                    tmp = [];
            location.search
                    //.replace ( "?", "" )
                    // this is better, there might be a question mark inside
                    .substr(1)
                    .split("&")
                    .forEach(function (item) {
                        tmp = item.split("=");
                        if (tmp[0] === val) result = decodeURIComponent(tmp[1]);
                    });
            return result;
        }
        var displayTotal=(parse('total')=='true')?true:false;
        console.log(displayTotal);
        function fetchJSONFile(path, callback) {
            var httpRequest = new XMLHttpRequest();
            httpRequest.onreadystatechange = function() {
                if (httpRequest.readyState === 4) {
                    if (httpRequest.status === 200) {
                        var data = JSON.parse(httpRequest.responseText);
                        if (callback) callback(data);
                    }
                }
            };
            httpRequest.open('GET', path);
            httpRequest.send();
        }

        function timeout(){
            fetchJSONFile('sentimentdata.json?total='+displayTotal, redrawMap);
        }
        function readyHandler() {
            drawingBuffer=1-drawingBuffer;
            buffers[1-drawingBuffer].style.visibility='hidden';
            buffers[drawingBuffer].style.visibility='visible';
        }
        function getCount(data){
            var count=0;
            for (i = 1; i < data.length; i++) {
                count+=data[i][1];
            }
            return count;
        }
        function redrawMap(jsondata) {
//            jsondata=[
//                    ['country','tweets'],
//                ['Bangladesh',2],
//                ['Malaysia',65],
//                ['Ecuador',465],
//                ['Peru',8],
//                ['Spain',260],
//                ['Slovenia',123],
//                ['Austria',7],
//                ['New Caledonia',3],
//                ['Israel',10],
//                ['Ukraine',88],
//                ['Saudi Arabia',193],
//                ['Kenya',40],
//                ['Egypt',96],
//                ['Iran',8],
//                ['Denmark',13],
//                ['Iraq',380],
//                ['Romania',66],
//                ['Liberia',3],
//                ['Hong Kong',7],
//                ['Morocco',173],
//                ['Portugal',104],
//                ['United States',9128],
//                ['Sri Lanka',1],
//                ['Canada',828],
//                ['Vietnam',23],
//                ['Macedonia',6],
//                ['Croatia',6],
//                ['Brazil',1323],
//                ['Georgia',3],
//                ['Mauritania',4],
//                ['Colombia',87],
//                ['Ireland',97],
//                ['Belgium',177],
//                ['Samoa',39],
//                ['Sweden',22],
//                ['United Kingdom',726],
//                ['Chile',449],
//                ['China',90],
//                ['Bosnia and Herzegovina',4],
//                ['Greece',644],
//                ['Russia',457],
//                ['Switzerland',51],
//                ['Bulgaria',2],
//                ['South Africa',55],
//                ['Uzbekistan',1],
//                ['Venezuela',248],
//                ['Armenia',15],
//                ['Greenland',232],
//                ['Kazakhstan',4],
//                ['Papua New Guinea',1],
//                ['India',95],
//                ['Thailand',86],
//                ['Estonia',2],
//                ['Kuwait',92],
//                ['Czech Republic',10],
//                ['Nepal',1],
//                ['Taiwan',2],
//                ['France',323],
//                ['Finland',9],
//                ['Turkey',129],
//                ['Indonesia',108],
//                ['Netherlands',605],
//                ['Lithuania',1],
//                ['Mexico',316],
//                ['Germany',52],
//                ['United Kingdom',25],
//                ['South Korea',141],
//                ['Pakistan',17],
//                ['Azerbaijan',8],
//                ['United Arab Emirates',55],
//                ['Latvia',4],
//                ['Poland',44],
//                ['Tonga',3],
//                ['New Zealand',13],
//                ['Solomon Islands',3],
//                ['Japan',1037],
//                ['Slovakia',1],
//                ['Afghanistan',7],
//                ['Zimbabwe',8],
//                ['Belarus',31],
//                ['Argentina',531],
//                ['Australia',103],
//                ['Italy',103],
//                ['Hungary',7],
//                ['Singapore',24],
//                ['Oman',24]];
            var totalCount=getCount(jsondata);
            var data = google.visualization.arrayToDataTable(jsondata);
            var options = {};
            options['dataMode'] = 'regions';
            options['width'] = window.screen.width;
            options['height'] = window.screen.height;
            options['backgroundColor'] = '#6698FF';

            if(displayTotal)
            {
                var countBox=document.getElementById('totalCount');
                countBox.style.top=window.screen.height-50;
                countBox.style.left=window.screen.width-500;
                countBox.style.visibility='visible';
                countBox.innerHTML='Total : '+totalCount;
                options['colorAxis'] = {'colors': ['#004400', '#00FF00']};
            }else {
                options['colorAxis'] = {'colors': ['#FF0000', '#FFFFFF', '#00FF00'], 'values': [-1, 0, 1]};
                options['legend']='none';
            }
            options['domain'] = 'IN';
            charts[1-drawingBuffer].draw(data, options);
            setTimeout(timeout,1000);
        }
        function drawMap() {
            buffers=[document.getElementById('buffer1'),document.getElementById('buffer2')];
            console.log(buffers);
            buffers[1-drawingBuffer].style.visibility='hidden';
            buffers[drawingBuffer].style.visibility='visible';
            charts=[new google.visualization.GeoChart(buffers[0]),new google.visualization.GeoChart(buffers[1])];
            google.visualization.events.addListener(charts[0], 'ready', readyHandler);
            google.visualization.events.addListener(charts[1], 'ready', readyHandler);
            timeout();
        };
    </script>
    <style>
        .buffer{
            position: absolute;
            height: 100%;
            width: 100%;
            visibility: hidden;
        }
        .count{
            position: relative;
            color:white;
            font-size: 200%;
            visibility: hidden;
        }
    </style>
</head>
<body>
<div id="buffer1" class="buffer"></div>
<div id="buffer2" class="buffer"></div>
<div id="totalCount" class="count">Total</div>
</body>
</html>