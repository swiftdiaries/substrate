// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"github.com/spf13/cobra"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/steps"
)

// Automation validates before cleanup without fetching credentials or building
// cluster clients. Keep this internal command on the deployment's validation.
var validateCmd = &cobra.Command{
	Use:               "validate",
	Hidden:            true,
	Args:              validateDeployArgs,
	PersistentPreRunE: func(*cobra.Command, []string) error { return nil },
	RunE: func(cmd *cobra.Command, _ []string) error {
		cfg, err := config.LoadFlags(cmd.Flags(), config.LoadOptions{ConfigPath: configPath})
		if err != nil {
			return err
		}
		return validateDeployConfig(&steps.Env{Cfg: cfg})
	},
}

func init() {
	rootCmd.AddCommand(validateCmd)
	config.BindCommandFlags(deployAteSystemPath, validateCmd.Flags())
	validateCmd.Flags().Lookup("setup-csi").NoOptDefVal = "none"
}
