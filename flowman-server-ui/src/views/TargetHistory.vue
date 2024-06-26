<template>
  <v-container
    fluid
  >
    <v-row>
      <v-col cols="12">
        <v-card shaped outlined elevation="2">
          <target-charts
            v-model="filter"
          />
        </v-card>
      </v-col>

      <v-col cols="12">
        <v-card shaped outlined elevation="2">
          <v-card-title>
            Target History
            <v-btn text rounded icon
                   @click="refresh"
            >
              <v-icon>refresh</v-icon>
            </v-btn>
          </v-card-title>

          <v-data-table
            :headers="headers"
            :items="targets"
            :loading="loading"
            :options.sync="options"
            :server-items-length="total"
            :expanded.sync="expanded"
            show-expand
            expand-icon="expand_more"
            item-key="id"
            @click:row="clickRow"
            :footer-props="{
              itemsPerPageOptions: [10, 25, 50, 100, -1],
              prevIcon: 'navigate_before',
              nextIcon: 'navigate_next',
              firstIcon: 'first_page',
              lastIcon: 'last_page',
              showFirstLastPage: true
            }"
          >
            <template v-slot:item.phase="{ item }">
              <phase :phase="item.phase"/>
            </template>
            <template v-slot:item.status="{ item }">
              <status :status="item.status"/>
            </template>
            <template v-slot:item.partitions="{ item }">
              <v-chip
                color="#aacccc"
                v-for="p in Object.entries(item.partitions) "
                :key="p[0]"
              >
                {{ p[0] }} : {{ p[1] }}
              </v-chip>
            </template>
            <template v-slot:expanded-item="{ item,headers }">
              <td :colspan="headers.length">
                <target-details :target="item.id"/>
              </td>
            </template>
          </v-data-table>
        </v-card>
      </v-col>
    </v-row>
  </v-container>
</template>

<script>
  import TargetCharts from "@/components/TargetCharts";
  import TargetDetails from "@/components/TargetDetails";
  import Filter from "@/mixins/Filter.js";
  import Status from '@/components/Status.vue'
  import Phase from '@/components/Phase.vue'
  import moment from "moment";

  export default {
    name: "TargetHistory",
    mixins: [Filter],
    components: {TargetCharts,TargetDetails,Phase,Status},

    data() {
      return {
        targets: [],
        expanded: [],
        total: 0,
        options: {},
        loading: false,
        headers: [
          {text: 'Target Run ID', value: 'id'},
          {text: 'Job Run ID', value: 'jobId'},
          {text: 'Project Name', value: 'project'},
          {text: 'Project Version', value: 'version'},
          {text: 'Target Name', value: 'target'},
          {text: 'Partition', value: 'partitions'},
          {text: 'Build Phase', value: 'phase'},
          {text: 'Status', value: 'status', width:120},
          {text: 'Started at', value: 'startDateTime'},
          {text: 'Finished at', value: 'endDateTime'},
          { text: 'Duration', value: 'duration' },
          {text: 'Error message', value: 'error', width:160},
        ]
      }
    },

    watch: {
      options: {
        handler () {
          this.getData()
        },
        deep: true,
      },
    },

    methods: {
      clickRow(item, event) {
        if(event.isExpanded) {
          const index = this.expanded.findIndex(i => i === item);
          this.expanded.splice(index, 1)
        } else {
          this.expanded.push(item);
        }
      },

      refresh() {
        this.options.page = 1
        this.getData()
      },

      getData() {
        const { page, itemsPerPage } = this.options
        const offset = (page-1)*itemsPerPage

        this.loading = true
        this.$api.getTargetsHistory(this.filter.projects, this.filter.jobs, this.filter.targets, this.filter.phases, this.filter.status, offset, itemsPerPage)
          .then(response => {
            let dateFormat = 'MMM D, YYYY HH:mm:ss'
            response.data.forEach(item => {
              let start = moment(item.startDateTime)
              let end = moment(item.endDateTime)
              item.startDateTime = start.format(dateFormat)
              item.endDateTime = end.format(dateFormat)
              item.duration = moment.duration(end.diff(start)).humanize()
            })
            this.targets = response.data
            this.total = response.total
            this.loading = false
          })
      },
    }
  }
</script>

<style>

</style>
