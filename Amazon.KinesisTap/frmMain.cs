/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 * 
 *  http://aws.amazon.com/apache2.0
 * 
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

using Amazon.KinesisTap.Core;
using Amazon.KinesisTap.Hosting;
using Amazon.KinesisTap.Windows;

namespace Amazon.KinesisTap
{
    public partial class frmMain : Form
    {
        private LogManager _logManger;

        public frmMain()
        {
            InitializeComponent();
        }

        private void btnStart_Click(object sender, EventArgs e)
        {
            btnStart.Enabled = false;
            //Off the UI thread
            Task.Run(() =>
            {
                _logManger = new LogManager(new NetTypeLoader(), new RegistryParameterStore());
                _logManger.Start();
            }).Wait();
            btnStop.Enabled = true;
        }

        private void btnStop_Click(object sender, EventArgs e)
        {
            btnStop.Enabled = false;
            //Off the UI thread
            Task.Run(() =>
            {
                _logManger?.Stop();
            }).Wait();
            btnStart.Enabled = true;
        }

        private void frmMain_FormClosing(object sender, FormClosingEventArgs e)
        {
        }
    }
}
