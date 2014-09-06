$(document).ready(function() {

/**
	$.ajax({
		url: "/isJobServerRunning"
	}).done(function(e) {
		console.log("JobSever is running");
		$("a#jobServerLink").addClass("label-success");
		
		$.ajax({
			url: "/registContext"
		}).done(function(e) {
			console.log(e);
		}).fail(function() {
			console.log(e);
		});

	}).fail(function() {
		console.log("JobSever is NOT running");
		$("a#jobServerLink").addClass("label-danger");
  	});
**/

	// 桁数指定の四捨五入
	function myRound(val, precision)
	{
	     digit = Math.pow(10, precision);
	     val = val * digit;
	     val = Math.round(val);
	     val = val / digit;
	     return val;
	}	


	$("#graphArea").height(window.innerHeight - 120);

	$("a.graphList").click(function (e) {
		e.preventDefault();

		$("svg").remove();
		$("#defaultPageRankList tr").remove();
		$("#dtPageRankList tr").remove();
		$("#diPageRankList tr").remove();
		$("#ccList tr").remove();

		var graphid = $("#" + e.target.id).attr("graphid");
		drawGraph(graphid);

		/**

		$("td.infoVal").html("<img class='loader' src='assets/images/ajax-loader.gif'/>");
		$("div#loaderAreaForPageRankDf").html("<img class='loader' src='assets/images/ajax-loader.gif'/>");
		$("div#loaderAreaForPageRankDi").html("<img class='loader' src='assets/images/ajax-loader.gif'/>");
		$("div#loaderAreaForPageRankDt").html("<img class='loader' src='assets/images/ajax-loader.gif'/>");
		$("div#loaderAreaForCC").html("<img class='loader' src='assets/images/ajax-loader.gif'/>");

		$.ajax({
			url: "/graph/basic/" + graphid
		})
		.always(function(e){
			$("td.infoVal img").remove();
		})
		.fail(function(e){
			console.log(e);
			$("td.infoVal").html("<span class='label label-danger>!</span>");
		})
		.done(function(e) {
			console.log(e);
			$("#edgesCount").html(e.result.edgesCount);
			$("#verticesCount").html(e.result.verticesCount);
			$("#maxInDegrees").html(e.result.maxInDegrees.degrees + "( ID : <a href='#' class='vertexid'>" + e.result.maxInDegrees.vertex + "</a> )");
			$("#maxOutDegrees").html(e.result.maxOutDegrees.degrees + "( ID : <a href='#' class='vertexid'>" + e.result.maxOutDegrees.vertex + "</a> )");
			$("#maxDegrees").html(e.result.maxDegrees.degrees + "( ID : <a href='#' class='vertexid'>" + e.result.maxDegrees.vertex + "</a> )");

			$.ajax({
				url: "/graph/default-pagerank/" + graphid
			})
			.always(function(e){
				$("div#loaderAreaForPageRankDf img").remove();
			})
			.fail(function(e){
				console.log(e);
				$("div#loaderAreaForPageRankDf").html("<span class='label label-danger>!</span>");
			})
			.done(function(e) {
				console.log(e);

				for(i=0; i<e.result.ranks.length; i++){
					var r = e.result.ranks[i].rank
					var v = e.result.ranks[i].vertex
					$("#defaultPageRankList").append("<tr><td><a href='#' class='vertexid'>" + v + "</a></td><td>" + myRound(r,5) + "</td></tr>");
				}

				$.ajax({
					url: "/graph/dodopipe-threshold-pagerank/" + graphid
				})
				.always(function(e){
					$("div#loaderAreaForPageRankDt img").remove();
				})
				.fail(function(e){
					console.log(e);
					$("div#loaderAreaForPageRankDt").html("<span class='label label-danger>!</span>");
				})
				.done(function(e) {
					console.log(e);

					for(i=0; i<e.result.ranks.length; i++){
						var r = e.result.ranks[i].rank
						var v = e.result.ranks[i].vertex
						$("#dtPageRankList").append("<tr><td><a href='#' class='vertexid'>" + v + "</a></td><td>" + myRound(r,5) + "</td></tr>");
					}

					$.ajax({
						url: "/graph/dodopipe-iteration-pagerank/" + graphid
					})
					.always(function(e){
						$("div#loaderAreaForPageRankDi img").remove();
					})
					.fail(function(e){
						console.log(e);
						$("div#loaderAreaForPageRankDi").html("<span class='label label-danger>!</span>");
					})
					.done(function(e) {
						console.log(e);

						for(i=0; i<e.result.ranks.length; i++){
							var r = e.result.ranks[i].rank
							var v = e.result.ranks[i].vertex
							$("#diPageRankList").append("<tr><td><a href='#' class='vertexid'>" + v + "</a></td><td>" + myRound(r,5) + "</td></tr>");
						}

						$.ajax({
							url: "/graph/connected-components/" + graphid
						})
						.always(function(e){
							$("div#loaderAreaForCC img").remove();
						})
						.fail(function(e){
							console.log(e);
							$("div#loaderAreaForCC").html("<span class='label label-danger>!</span>");
						})
						.done(function(e) {
							console.log(e);

							for(i=0; i<e.result.connectedComponents.length; i++){
								var c = e.result.connectedComponents[i].cc
								var v = e.result.connectedComponents[i].vertex
								$("#ccList").append("<tr><td>" + c + "</td><td><a href='#' class='vertexidList'>" + v + "</a></td></tr>");
							}

							$.ajax({
								url: "/graph/subgraph-neighbors/" + graphid
							})
							.always(function(e){
								//$("div#loaderAreaForCC img").remove();
							})
							.fail(function(e){
								console.log(e);
								//$("div#loaderAreaForCC").html("<span class='label label-danger>!</span>");
							})
							.done(function(e) {
								console.log(e);

								// for(i=0; i<e.result.connectedComponents.length; i++){
								// 	var c = e.result.connectedComponents[i].cc
								// 	var v = e.result.connectedComponents[i].vertex
								// 	$("#ccList").append("<tr><td>" + c + "</td><td><a href='#' class='vertexidList'>" + v + "</a></td></tr>");
								// }
							});
						});
					});
				});
			});
		});
		**/
	});

	$( document ).on( "click", "a.vertexid", function (e) {
		e.preventDefault();
	});

	$( document ).on( "mouseover", "a.vertexid", function (e) {
		e.preventDefault();
		toggleNodeCircleColor(e.target.innerText, "#ff0000")
	});

	$( document ).on( "mouseout", "a.vertexid", function (e) {
		e.preventDefault();
		toggleNodeCircleColor(e.target.innerText, "#333")
	});

	$( document ).on( "mouseover", "a.vertexidList", function (e) {
		e.preventDefault();
		var ids = e.target.innerText.split(",");
		for(i=0; i < ids.length; i++){
			toggleNodeCircleColor(ids[i], "#ff0000")
		}
	});

	$( document ).on( "mouseout", "a.vertexidList", function (e) {
		e.preventDefault();
		var ids = e.target.innerText.split(",");
		for(i=0; i < ids.length; i++){
			toggleNodeCircleColor(ids[i], "#333")
		}
	});

	function toggleNodeCircleColor(vertexid, color) {
		var circle = $("svg g text").filter(function (){
			return $(this).text() == vertexid;
		}).parent();
		circle.children(1).css("fill", color);
	}
});


function drawGraph(graphid){
	d3.csv("/graph/csv/" + graphid, function(error, links) {
	//d3.csv("assets/sample_graph.csv", function(error, links) {

		var nodes = {};

		// Compute the distinct nodes from the links.
		links.forEach(function(link) {
			link.source = nodes[link.source] || (nodes[link.source] = {name: link.source});
			link.target = nodes[link.target] || (nodes[link.target] = {name: link.target});
			link.value = +link.value;
		});

		console.log(" ***** links ***** ")
		console.log(links);
		console.log(" ***** nodes ***** ")
		console.log(nodes);

		// var width = window.innerWidth, height = (window.innerHeight - 100);
		var width = $("#graphArea").width(), height = $("#graphArea").height();

		console.log("width : " + width);
		console.log("height : " + height);


		var force = d3.layout.force()
		    .nodes(d3.values(nodes))
		    .links(links)
		    .size([width, height])
		    .linkDistance(100)
		    // .linkDistance(function(d){
		    //   //console.log(d);
		    //   return d.value * 100;
		    // })
	//	    .charge(-300)
			.gravity(0) //可能か限り動きを少なくする
		    .on("tick", tick)
		    .start();

		var svg = d3.select("#graphArea").append("svg")
		    .attr("width", width)
		    .attr("height", height);

		// build the arrow.
		svg.append("svg:defs").selectAll("marker")
		    .data(["end"])
		    .enter().append("svg:marker")
		    .attr("id", String)
		    .attr("viewBox", "0 -5 10 10")
		    .attr("refX", 15)
		    .attr("refY", -1.5)
		    .attr("markerWidth", 6)
		    .attr("markerHeight", 6)
		    .attr("orient", "auto")
		    .append("svg:path")
		    .attr("d", "M0,-5L10,0L0,5");
		
		// add the links and the arrows
		var path = svg.append("svg:g").selectAll("path")
		    .data(force.links())
		    .enter().append("svg:path")
		    .attr("class", "link")
		    .attr("marker-end", "url(#end)");
		
		// define the nodes
		var node = svg.selectAll(".node")
		    .data(force.nodes())
		    .enter().append("g")
		    .attr("class", "node")
		    .call(force.drag); // allow to be draged
		
		// add the nodes
		node.append("circle")
		    .attr("r", 5);
		
		// add the text 
		node.append("text")
		    .attr("x", 12)
		    .attr("dy", ".35em")
		    .text(function(d) { return d.name; });
		
		// add the curvy lines
		function tick() {
		    path.attr("d", function(d) {
		        var dx = d.target.x - d.source.x,
		            dy = d.target.y - d.source.y,
		            dr = Math.sqrt(dx * dx + dy * dy);
				var ret =  "M" + 
		            d.source.x + "," + 
		            d.source.y + "A" + 
		            dr + "," + dr + " 0 0,1 " + 
		            d.target.x + "," + 
		            d.target.y;
		        //console.log("tick (path) : " + ret);
		        return ret;
		    });

		    node.attr("transform", function(d) { 
				var ret =  "translate(" + d.x + "," + d.y + ")";
		        //console.log("tick (attr) : " + ret);
		        return ret;
			});
		}

	});
}
