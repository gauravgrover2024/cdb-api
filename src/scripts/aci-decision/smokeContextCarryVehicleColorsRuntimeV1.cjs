#!/usr/bin/env node
'use strict';

require('dotenv/config');
const assert = require('assert');

const asArray = (value) => (Array.isArray(value) ? value.filter(Boolean) : []);
const lower = (value) => String(value || '').toLowerCase();

const getRows = (response = {}) =>
  asArray(response.rows).length
    ? asArray(response.rows)
    : asArray(response.data?.rows).length
      ? asArray(response.data.rows)
      : asArray(response.colors).length
        ? asArray(response.colors)
        : asArray(response.data?.colors);

const getSelectedVehicle = (response = {}) =>
  response.selectedVehicle ||
  response.contextPatch?.selectedVehicle ||
  response.contextPatch?.contextState?.selectedVehicle ||
  response.contextPatch?.aciContextState?.selectedVehicle ||
  response.data?.selectedVehicle ||
  {};

(async () => {
  const mongooseModule = await import('mongoose');
  const mongoose = mongooseModule.default || mongooseModule;
  const connectDbModule = await import('../../config/db.js');
  const connectDB = connectDbModule.default || connectDbModule;
  const agentModule = await import('../../services/aiAgent/aiAgent.service.js');
  const { chatWithAgent } = agentModule;

  await connectDB();

  try {
    const first = await chatWithAgent({
      message: 'Thar Roxx price New Delhi',
      context: {},
    });

    const firstSelected = getSelectedVehicle(first);
    assert(/thar\s*roxx/i.test(`${first.title || ''} ${first.answer || ''}`), 'price step should resolve Thar Roxx');
    assert(firstSelected.model || firstSelected.fullModel || firstSelected.modelKey, 'price step should produce selectedVehicle');

    const firstPatch = first.contextPatch || {};
    assert(firstPatch.contextState?.selectedVehicle?.model || firstPatch.contextState?.selectedVehicle?.fullModel || firstPatch.contextState?.selectedVehicle?.modelKey, 'contextPatch.contextState.selectedVehicle must be durable');
    assert(firstPatch.aciContextState?.selectedVehicle?.model || firstPatch.aciContextState?.selectedVehicle?.fullModel || firstPatch.aciContextState?.selectedVehicle?.modelKey, 'contextPatch.aciContextState.selectedVehicle must be durable');

    const followContext = {
      ...firstPatch,
      contextState: firstPatch.contextState || firstPatch.aciContextState || {},
      aciContextState: firstPatch.aciContextState || firstPatch.contextState || {},
      selectedVehicle: firstPatch.selectedVehicle || firstSelected,
      anchorCity: firstPatch.anchorCity || 'new-delhi',
    };

    const second = await chatWithAgent({
      message: 'colors',
      context: followContext,
    });

    const direct = await chatWithAgent({
      message: 'colors',
      context: {
        selectedVehicle: {
          make: 'Mahindra',
          makeKey: 'mahindra',
          model: 'Thar Roxx',
          modelKey: 'thar-roxx',
          fullModel: 'Mahindra Thar Roxx',
          city: 'New Delhi',
          citySlug: 'new-delhi',
        },
        anchorCity: 'new-delhi',
        contextState: {
          selectedVehicle: {
            make: 'Mahindra',
            makeKey: 'mahindra',
            model: 'Thar Roxx',
            modelKey: 'thar-roxx',
            fullModel: 'Mahindra Thar Roxx',
            city: 'New Delhi',
            citySlug: 'new-delhi',
          },
          anchors: {
            primaryVehicle: {
              make: 'Mahindra',
              makeKey: 'mahindra',
              model: 'Thar Roxx',
              modelKey: 'thar-roxx',
              fullModel: 'Mahindra Thar Roxx',
            },
          },
        },
      },
    });

    for (const [label, response] of [['followup', second], ['direct', direct]]) {
      const blob = `${response.title || ''} ${response.answer || ''} ${JSON.stringify(getSelectedVehicle(response))}`;
      assert.strictEqual(response.intent || response.tool, 'vehicle_colors', `${label}: colors should route to vehicle_colors`);
      assert(/thar\s*roxx/i.test(blob), `${label}: response should resolve Thar Roxx`);
      assert(getRows(response).length > 0, `${label}: color rows should exist`);
      assert.notStrictEqual(response.tool, 'clarification', `${label}: should not ask clarification`);
      assert(!/which car should i check this for/i.test(lower(response.answer)), `${label}: should not ask which car`);
    }

    console.log(JSON.stringify({
      suite: 'ACI context carry vehicle colors runtime smoke v1',
      ok: true,
      first: {
        intent: first.intent,
        title: first.title,
        selectedVehicle: firstSelected,
        contextStateSelectedVehicle: firstPatch.contextState?.selectedVehicle || null,
      },
      followup: {
        intent: second.intent,
        title: second.title,
        rows: getRows(second).length,
        selectedVehicle: getSelectedVehicle(second),
      },
      direct: {
        intent: direct.intent,
        title: direct.title,
        rows: getRows(direct).length,
        selectedVehicle: getSelectedVehicle(direct),
      },
    }, null, 2));
  } finally {
    await mongoose.disconnect();
  }
})().catch((error) => {
  console.error(JSON.stringify({
    suite: 'ACI context carry vehicle colors runtime smoke v1',
    ok: false,
    error: error.message,
  }, null, 2));
  process.exit(1);
});
