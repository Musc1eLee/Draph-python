var FIRST_CLICK = '';
var LAST_CLICK = '';
var CHOOSE_ID = '';

var display_topo = function (ret) {
	var allnodes = ret['all_nodes'];
	var alledges = ret["all_edges"];

	// 创建节点对象
	var nodes = new vis.DataSet(allnodes);
	// 创建连线对象
	var edges = new vis.DataSet(alledges);
	// 创建一个网络拓扑图
	var container = document.getElementById('mynetwork');
	var data = {nodes: nodes, edges: edges};
	var options = {
		edges: {
			color: {
				color: '#C3C3C3',
				highlight: '#3698EA',
				hover: '#3698EA',
			},
			hoverWidth: 2,
		},
		// 设置一个组，可以显示不同的图片
		groups: {
			useDefaultGroups: true,
			route_switch: {
				shape: 'image',
				image: '/router.svg',
			},
			route_switch_red: {
				shape: 'image',
				image: '/router_red.svg',
			},
			net_camera: {
				shape: 'image',
				image: '/camera.svg',
			},
			net_camera_red: {
				shape: 'image',
				image: '/camera_red.svg',
			},
			net_storage: {
				shape: 'image',
				image: '/storage.svg'
			},
			security: {
				shape: 'image',
				image: '/security.svg'
			},
			server: {
				shape: 'image',
				image: '/server.svg'
			},
			industrial_control: {
				shape: 'image',
				image: '/industrial.svg'
			},
			computer: {
				shape: 'image',
				image: '/computer.svg'
			},
			other_device: {
				shape: 'image',
				image: '/other.svg'
			},
			net_storage_red: {
				shape: 'image',
				image: '/storage_red.svg'
			},
			security_red: {
				shape: 'image',
				image: '/security_red.svg'
			},
			server_red: {
				shape: 'image',
				image: '/server_red.svg'
			},
			industrial_control_red: {
				shape: 'image',
				image: '/industrial_red.svg'
			},
			computer_red: {
				shape: 'image',
				image: '/computer_red.svg'
			},
			other_device_red: {
				shape: 'image',
				image: '/other_red.svg'
			}
		},
		interaction: {
			hover: true,
			// keyboard: {
			//     enabled: false,
			//     speed: {x: 10, y: 10, zoom: 0.02},
			//     bindToWindow: true
			// },
			// navigationButtons: true
		},
		physics: {
			adaptiveTimestep: true,
			maxVelocity: 146,
			forceAtlas2Based: {
				// gravitationalConstant: -1000,
				// 引力参数，影响了节点之间的互斥现象
				centralGravity: 0.001,
				springConstant: 0.03,  //弹簧的硬度
				springLength: 200,
			},
			solver: 'forceAtlas2Based',
			stabilization: {
				// 这个配置严重影响加载速度
				iterations: 10
			}
		},
		layout: {
			improvedLayout: true, //开启后节点的子节点存在数量过多时自动闭合
			// clusterThreshold: 50,
		},
		// manipulation: {
		//     enabled: true,
		//     initiallyActive: false,
		// }
	};
	var network = new vis.Network(container, data, options);

	network.on('doubleClick', function (params) {//循环隐藏或显示子节点和子从节点,展开只展开下一级，提高性能,考虑收缩下一级，提高性能
		try {
			let choose_id = network.body.data.nodes._data[params.nodes];
			if (choose_id) {
				change_sid_state(choose_id.subids);
			}
		} catch {
			App.initToastr({'visiable': true, 'type': "warning", "msg": "该节点不可展开或收缩"});
		} finally {
			network.body.emitter.emit('_dataChanged');
			network.redraw();
		}
	});

	var get_state = function (ids) {
		var state = true;
		for (let i of ids) {
			let sub_state = network.body.data.nodes._data[i].hidden;
			if (!sub_state) {
				state = false;
				break;
			}
		}
		return state;
	};


	var change_sid_state = function (ids) {
		/**
		 * 	展开或收缩本级节点，性能较高
		 * */
		if (ret.is_query) {
			for (var i of ids) {
				const sub_id = network.body.data.nodes._data[i].subids;
				const sub_hidden = network.body.data.nodes._data[i].hidden;
				if (sub_id == undefined) {
					network.clustering.updateClusteredNode(i, {hidden: !sub_hidden});
				}
				network.body.data.nodes._data[i].hidden = !sub_hidden;
			}
			;
		} else {
			for (let i of ids) {
				let sub_id = network.body.data.nodes._data[i].subids;
				if (sub_id == undefined) {
					let state = network.body.data.nodes._data[i].hidden;
					network.clustering.updateClusteredNode(i, {hidden: !state});
					network.body.data.nodes._data[i].hidden = !state;
				} else {
					let hidden_state = get_state(sub_id);
					if (hidden_state) {
						let state = network.body.data.nodes._data[i].hidden;
						network.clustering.updateClusteredNode(i, {hidden: !state});
						network.body.data.nodes._data[i].hidden = !state;
					}
				}
			}
		}
		/**
		 * 展开或收缩本级以及所有的子节点、子从节点，性能较差
		 * **/
		// let i = ids[0];
		// var is_full_up = (network.body.data.nodes._data[i]).hidden  //获取子节点的显示状态
		// for (let i of ids) {
		// 	network.clustering.updateClusteredNode(i, {hidden: !is_full_up});
		// 	network.body.data.nodes._data[i].hidden = !is_full_up;
		// 	let sid = (network.body.data.nodes._data[i]).subids;
		// 	if (sid) {
		// 		change_sid_state(sid, flag = false);
		// 	}
		// }
	};

	network.on('click', function (params) {   //鼠标单击事件

		if (FIRST_CLICK == '') {
			FIRST_CLICK = (new Date()).valueOf();
		} else if (LAST_CLICK == '') {
			LAST_CLICK = (new Date()).valueOf();
		} else {
			FIRST_CLICK = LAST_CLICK;
			LAST_CLICK = (new Date()).valueOf();
		}
		var choose_id = network.body.data.nodes._data[params.nodes];
		if (LAST_CLICK - FIRST_CLICK >= 1500 || !LAST_CLICK || choose_id != CHOOSE_ID) {
			CHOOSE_ID = choose_id
			if (choose_id) {
				let choose_ip_str = choose_id.label;
				//添加单击事件
			}
			if (FIRST_CLICK && LAST_CLICK) {
				FIRST_CLICK = '';
				LAST_CLICK = '';
			}
		}
	});
}
