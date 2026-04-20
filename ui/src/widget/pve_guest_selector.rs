use std::rc::Rc;

use anyhow::Error;
use yew::{
    html,
    html::{IntoEventCallback, IntoPropValue},
    virtual_dom::Key,
    AttrValue, Callback, Component, Properties,
};

use pwt::{
    css::FlexFit,
    props::{FieldBuilder, LoadCallback, WidgetBuilder, WidgetStyleBuilder},
    state::Store,
    tr,
    widget::{
        data_table::{DataTable, DataTableColumn, DataTableHeader},
        form::{Selector, SelectorRenderArgs},
        GridPicker,
    },
};
use pwt_macros::{builder, widget};

#[derive(Clone, PartialEq)]
struct PveGuestEntry {
    vmid: u32,
    name: String,
    node: String,
    status: String,
    guest_type: String,
}

#[widget(comp=PveGuestSelectorComp, @input)]
#[derive(Clone, Properties, PartialEq)]
#[builder]
pub struct PveGuestSelector {
    /// The default value
    #[builder(IntoPropValue, into_prop_value)]
    #[prop_or_default]
    pub default: Option<AttrValue>,

    /// Change callback
    #[builder_cb(IntoEventCallback, into_event_callback, Option<AttrValue>)]
    #[prop_or_default]
    pub on_change: Option<Callback<Option<AttrValue>>>,

    /// The remote to select the guest from
    #[builder(IntoPropValue, into_prop_value)]
    #[prop_or_default]
    pub remote: AttrValue,

    /// Optional node filter for the guest list
    #[builder(IntoPropValue, into_prop_value)]
    #[prop_or_default]
    pub node: Option<AttrValue>,

    /// Guest type filter (`qemu` or `lxc`)
    #[builder(IntoPropValue, into_prop_value)]
    #[prop_or_default]
    pub guest_type: AttrValue,
}

impl PveGuestSelector {
    pub fn new(remote: impl IntoPropValue<AttrValue>) -> Self {
        yew::props!(Self {
            remote: remote.into_prop_value()
        })
    }
}

pub struct PveGuestSelectorComp {
    store: Store<PveGuestEntry>,
    load_callback: LoadCallback<Vec<PveGuestEntry>>,
}

impl PveGuestSelectorComp {
    async fn get_guest_list(
        remote: AttrValue,
        node: Option<AttrValue>,
        guest_type: AttrValue,
    ) -> Result<Vec<PveGuestEntry>, Error> {
        if remote.is_empty() {
            return Ok(Vec::new());
        }

        let node_filter = node
            .map(|value| value.to_string())
            .filter(|value| !value.trim().is_empty());
        let filter_guest_type = if guest_type.is_empty() {
            "qemu".to_string()
        } else {
            guest_type.to_lowercase()
        };

        let node_arg = node_filter.as_deref();
        let mut guests = Vec::new();

        let node_text = node_filter.clone().unwrap_or_else(|| "-".to_string());

        if filter_guest_type == "lxc" {
            for item in crate::pdm_client().pve_list_lxc(&remote, node_arg).await? {
                guests.push(PveGuestEntry {
                    vmid: item.vmid,
                    name: item.name.unwrap_or_else(|| "-".to_string()),
                    node: node_text.clone(),
                    status: item.status.to_string(),
                    guest_type: "lxc".to_string(),
                });
            }
        } else {
            for item in crate::pdm_client().pve_list_qemu(&remote, node_arg).await? {
                guests.push(PveGuestEntry {
                    vmid: item.vmid,
                    name: item.name.unwrap_or_else(|| "-".to_string()),
                    node: node_text.clone(),
                    status: item.status.to_string(),
                    guest_type: "qemu".to_string(),
                });
            }
        }

        guests.sort_by(|a, b| a.vmid.cmp(&b.vmid).then_with(|| a.node.cmp(&b.node)));
        Ok(guests)
    }

    fn create_load_callback(ctx: &yew::Context<Self>) -> LoadCallback<Vec<PveGuestEntry>> {
        let props = ctx.props();
        let remote = props.remote.clone();
        let node = props.node.clone();
        let guest_type = props.guest_type.clone();

        (move || Self::get_guest_list(remote.clone(), node.clone(), guest_type.clone())).into()
    }
}

impl Component for PveGuestSelectorComp {
    type Message = ();
    type Properties = PveGuestSelector;

    fn create(ctx: &yew::Context<Self>) -> Self {
        Self {
            store: Store::with_extract_key(|entry: &PveGuestEntry| {
                Key::from(entry.vmid.to_string())
            }),
            load_callback: Self::create_load_callback(ctx),
        }
    }

    fn changed(&mut self, ctx: &yew::Context<Self>, old_props: &Self::Properties) -> bool {
        let props = ctx.props();
        if old_props.remote != props.remote
            || old_props.node != props.node
            || old_props.guest_type != props.guest_type
        {
            self.load_callback = Self::create_load_callback(ctx);
        }
        true
    }

    fn view(&self, ctx: &yew::Context<Self>) -> yew::Html {
        let props = ctx.props();

        let on_change = {
            let on_change = props.on_change.clone();
            let store = self.store.clone();
            move |key: Key| {
                if let Some(on_change) = &on_change {
                    let result = store
                        .read()
                        .iter()
                        .find(|entry| key == store.extract_key(entry))
                        .map(|entry| entry.vmid.to_string().into());
                    on_change.emit(result);
                }
            }
        };

        Selector::new(
            self.store.clone(),
            move |args: &SelectorRenderArgs<Store<PveGuestEntry>>| {
                GridPicker::new(
                    DataTable::new(columns(), args.store.clone())
                        .min_width(520)
                        .header_focusable(false)
                        .class(FlexFit),
                )
                .selection(args.selection.clone())
                .on_select(args.controller.on_select_callback())
                .into()
            },
        )
        .loader(self.load_callback.clone())
        .with_std_props(&props.std_props)
        .with_input_props(&props.input_props)
        .editable(true)
        .on_change(on_change)
        .default(props.default.clone())
        .into()
    }
}

fn columns() -> Rc<Vec<DataTableHeader<PveGuestEntry>>> {
    Rc::new(vec![
        DataTableColumn::new(tr!("VMID"))
            .render(|entry: &PveGuestEntry| html! {entry.vmid})
            .sorter(|a: &PveGuestEntry, b: &PveGuestEntry| a.vmid.cmp(&b.vmid))
            .sort_order(true)
            .into(),
        DataTableColumn::new(tr!("Name"))
            .get_property(|entry: &PveGuestEntry| &entry.name)
            .into(),
        DataTableColumn::new(tr!("Node"))
            .get_property(|entry: &PveGuestEntry| &entry.node)
            .into(),
        DataTableColumn::new(tr!("Status"))
            .get_property(|entry: &PveGuestEntry| &entry.status)
            .into(),
        DataTableColumn::new(tr!("Type"))
            .get_property(|entry: &PveGuestEntry| &entry.guest_type)
            .into(),
    ])
}
