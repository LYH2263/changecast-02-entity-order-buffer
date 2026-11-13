<script setup>
import { onMounted, ref } from 'vue'
import { getJSON } from '../api'
const settings = ref({})
const err = ref('')
onMounted(async () => {
  try { settings.value = await getJSON('/api/settings') } catch (e) { err.value = String(e.message || e) }
})
</script>
<template>
  <div class="page">
    <h1>设置</h1>
    <p class="muted">全局策略展示；每流缓冲上限、等待时长与起始 seq 在「投影流」页按流配置。</p>
    <p v-if="err" class="tag danger">{{ err }}</p>
    <div class="panel">
      <table>
        <thead><tr><th>键</th><th>值</th></tr></thead>
        <tbody>
          <tr v-for="(v, k) in settings" :key="k">
            <td class="mono">{{ k }}</td>
            <td><span class="tag">{{ v }}</span></td>
          </tr>
        </tbody>
      </table>
    </div>
    <div class="panel">
      <h3 style="margin-top:0">本切口已选定的互斥策略</h3>
      <ul style="margin:0;padding-left:1.2rem;line-height:1.9">
        <li><b>保序粒度</b>：按实体键局部保序（<span class="mono">per_entity_seq</span>），不做全流统一时间窗对账。</li>
        <li><b>逾限策略</b>：<span class="mono">block</span> —— 实体缓冲逾限即阻塞该实体后续应用，直至人工清缓冲或补齐缺口；事件不丢弃（丢弃计数恒 0），他实体不整批卡死。</li>
        <li><b>迟到低 seq</b>：判过期跳过（<span class="mono">skipped_stale</span>），绝不回盖已应用的更高 seq。</li>
        <li><b>清缓冲</b>：显式允许缓冲内最小 seq 为新起始 seq，缺口计数（<span class="mono">gap_skipped</span>）后按实体内 seq 连续放出。</li>
      </ul>
    </div>
  </div>
</template>
