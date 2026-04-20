use std::rc::Rc;

use anyhow::Error;
use proxmox_yew_comp::rrd_value_renderer;
use yew::{
    AttrValue, Callback, Component, Properties, html,
    html::{IntoEventCallback, IntoPropValue},
    virtual_dom::Key,
};

use pwt::{
    css::FlexFit,
    props::{FieldBuilder, LoadCallback, WidgetBuilder, WidgetStyleBuilder},
    state::Store,
    tr,
    widget::{
        GridPicker,
        data_table::{DataTable, DataTableColumn, DataTableHeader},
        form::{Selector, SelectorRenderArgs},
    },
};
use pwt_macros::{builder, widget};

use pdm_client::types::ClusterNodeIndexResponse;

#[widget(comp=PveNodeSelectorComp, @input)]
#[derive(Clone, Properties, PartialEq)]
#[builder]
pub struct PveNodeSelector {
    /// The default value
    #[builder(IntoPropValue, into_prop_value)]
    #[prop_or_default]
    pub default: Option<AttrValue>,

    /// Change callback
    #[builder_cb(IntoEventCallback, into_event_callback, Option<AttrValue>)]
    #[prop_or_default]
    pub on_change: Option<Callback<Option<AttrValue>>>,

    /// The remote to select the nodes from
    #[builder(IntoPropValue, into_prop_value)]
    #[prop_or_default]
    pub remote: AttrValue,

    /// Node names that should not appear in the selector (e.g. nodes that already have a
    /// subscription key assigned in the pool).
    #[prop_or_default]
    pub excluded_nodes: Rc<Vec<String>>,

    /// Whether to show the resource-utilization columns ("CPU Usage", "Memory Usage"). Callers
    /// picking a node for a context where utilization is irrelevant (e.g. subscription
    /// assignment) can hide them.
    #[builder]
    #[prop_or(true)]
    pub show_memory: bool,

    /// Source node of the guest. Used by the migration dialog to hide the node from the
    /// target list, so the user can only pick a different node as the migration target.
    #[builder(IntoPropValue, into_prop_value)]
    #[prop_or_default]
    pub source_node: Option<AttrValue>,
}

impl PveNodeSelector {
    pub fn new(remote: impl IntoPropValue<AttrValue>) -> Self {
        yew::props!(Self {
            remote: remote.into_prop_value()
        })
    }

    pub fn excluded_nodes(mut self, nodes: Rc<Vec<String>>) -> Self {
        self.excluded_nodes = nodes;
        self
    }
}

pub struct PveNodeSelectorComp {
    store: Store<ClusterNodeIndexResponse>,
    load_callback: LoadCallback<Vec<ClusterNodeIndexResponse>>,
}

impl PveNodeSelectorComp {
    async fn get_node_list(
        remote: AttrValue,
        excluded: Rc<Vec<String>>,
        source_node: Option<AttrValue>,
    ) -> Result<Vec<ClusterNodeIndexResponse>, Error> {
        // An empty remote is a valid "no data yet" state for dependent selectors.
        if remote.is_empty() {
            return Ok(Vec::new());
        }

        let source_node = source_node.as_deref().map(str::to_string);
        let mut nodes = crate::pdm_client().pve_list_nodes(&remote).await?;
        nodes.retain(|node| {
            !excluded.iter().any(|excluded| excluded == &node.node)
                && source_node.as_deref() != Some(node.node.as_str())
        });
        nodes.sort_by(|a, b| a.node.cmp(&b.node));
        Ok(nodes)
    }

    fn create_load_callback(
        ctx: &yew::Context<Self>,
    ) -> LoadCallback<Vec<ClusterNodeIndexResponse>> {
        let props = ctx.props();
        let remote = props.remote.clone();
        let excluded = props.excluded_nodes.clone();
        let source_node = props.source_node.clone();

        // The selector owns the loading lifecycle. Rebuilding this callback on dependent prop
        // changes avoids stale node lists in forms that first select a remote, then a node.
        (move || Self::get_node_list(remote.clone(), excluded.clone(), source_node.clone())).into()
    }
}

impl Component for PveNodeSelectorComp {
    type Message = ();
    type Properties = PveNodeSelector;

    fn create(ctx: &yew::Context<Self>) -> Self {
        Self {
            store: Store::with_extract_key(|node: &ClusterNodeIndexResponse| {
                Key::from(node.node.as_str())
            }),
            load_callback: Self::create_load_callback(ctx),
        }
    }

    fn changed(&mut self, ctx: &yew::Context<Self>, old_props: &Self::Properties) -> bool {
        let props = ctx.props();
        if old_props.remote != props.remote
            || old_props.excluded_nodes != props.excluded_nodes
            || old_props.source_node != props.source_node
        {
            self.load_callback = Self::create_load_callback(ctx);
        }
        true
    }

    fn view(&self, ctx: &yew::Context<Self>) -> yew::Html {
        let props = ctx.props();
        let show_memory = props.show_memory;
        let source_node = props.source_node.clone();
        let on_change = {
            let on_change = props.on_change.clone();
            let store = self.store.clone();
            move |key: Key| {
                if let Some(on_change) = &on_change {
                    let result = store
                        .read()
                        .iter()
                        .find(|e| key == store.extract_key(e))
                        .map(|e| e.node.clone().into());
                    on_change.emit(result);
                }
            }
        };
        Selector::new(self.store.clone(), {
            move |args: &SelectorRenderArgs<Store<ClusterNodeIndexResponse>>| {
                GridPicker::new(
                    DataTable::new(columns(show_memory), args.store.clone())
                        .min_width(300)
                        .header_focusable(false)
                        .class(FlexFit),
                )
                .selection(args.selection.clone())
                .on_select(args.controller.on_select_callback())
                .into()
            }
        })
        .loader(self.load_callback.clone())
        .with_std_props(&props.std_props)
        .with_input_props(&props.input_props)
        .autoselect(source_node.is_none())
        .editable(true)
        .on_change(on_change)
        .default(props.default.clone())
        .into()
    }
}

fn columns(show_memory: bool) -> Rc<Vec<DataTableHeader<ClusterNodeIndexResponse>>> {
    let mut columns = vec![
        DataTableColumn::new(tr!("Node"))
            .get_property(|entry: &ClusterNodeIndexResponse| &entry.node)
            .sort_order(true)
            .into(),
    ];
    if show_memory {
        columns.push(
            DataTableColumn::new(tr!("CPU Usage"))
                .render(|entry: &ClusterNodeIndexResponse| match entry.cpu {
                    Some(cpu) => html! { rrd_value_renderer::render_cpu_usage(&cpu) },
                    None => html! {},
                })
                .sorter(
                    |a: &ClusterNodeIndexResponse, b: &ClusterNodeIndexResponse| {
                        // total_cmp tolerates NaN; preserve the "no data sorts low" intuition by
                        // mapping None to negative infinity so unprobed nodes stay at the bottom.
                        a.cpu
                            .unwrap_or(f64::NEG_INFINITY)
                            .total_cmp(&b.cpu.unwrap_or(f64::NEG_INFINITY))
                    },
                )
                .into(),
        );
        columns.push(
            DataTableColumn::new(tr!("Memory Usage"))
                .render(
                    |entry: &ClusterNodeIndexResponse| match (entry.mem, entry.maxmem) {
                        (Some(mem), Some(maxmem)) => {
                            html! {format!("{:.2}%", 100.0 * mem as f64 / maxmem as f64)}
                        }
                        _ => html! {},
                    },
                )
                .sorter(
                    |a: &ClusterNodeIndexResponse, b: &ClusterNodeIndexResponse| a.mem.cmp(&b.mem),
                )
                .into(),
        );
    }
    Rc::new(columns)
}
